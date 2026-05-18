"""Graph workflow executor — v12
Architecture: pure orchestration. All node logic lives in app/nodes/.
New in v12:
  - Parallel branch execution: independent branches run concurrently via ThreadPoolExecutor
  - Level-based scheduling: nodes grouped into dependency levels; all nodes in a level run
    in parallel (reads from context are safe — only predecessors' keys are read)
  - EXECUTOR_PARALLEL=true env var or graph_data['parallel']=True to enable
  - EXECUTOR_MAX_WORKERS env var to cap thread pool size (default 8)
  - _run_one_node() extracted for use by the single-node test endpoint
"""
import os
import json
import time
import logging
import psycopg2
import redis.exceptions
from json import JSONDecodeError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

log = logging.getLogger(__name__)



class LoggingLoggerAdapter:
    """"Wrap a bare callable or a Logger into a unified logger interface.

    Supports BOTH call patterns that appear across the codebase:
      - logger(msg)                 ← bare callable (print, messages.append, etc.)
      - logger.info(msg, ...)       ← logging.Logger method style

    When the wrapped target is a Logger, pass through all method calls directly.
    When it's a bare callable, only allow .info() (used by action nodes) and .warning()
    (used by some routers) — all other attributes raise AttributeError so misuse
    is loudly visible rather than silently ignored.
    """
    def __init__(self, target):
        self._target = target
        self._is_logger = isinstance(target, logging.Logger)

    def info(self, msg, *args, **kwargs):
        if self._is_logger:
            self._target.info(msg, *args, **kwargs)
        else:
            # Bare callable — format as single string like Logger does
            msg_str = msg % args if args else msg
            self._target(msg_str)

    def warning(self, msg, *args, **kwargs):
        if self._is_logger:
            self._target.warning(msg, *args, **kwargs)
        else:
            msg_str = msg % args if args else msg
            self._target(msg_str)

    def error(self, msg, *args, **kwargs):
        if self._is_logger:
            self._target.error(msg, *args, **kwargs)
        else:
            msg_str = msg % args if args else msg
            self._target(msg_str)

    def __repr__(self):
        return f"<LoggingLoggerAdapter({self._target!r})>"

# ── re-export _render for backward compatibility (call_graph etc) ──────────
from app.nodes._utils import _render  # noqa: F401

# ── OpenTelemetry (noop when package absent or OTLP endpoint unset) ──────────
# Import from app.telemetry — it provides real impls or safe noops depending
# on whether opentelemetry-api is installed.
from app.telemetry import get_tracer as _get_tracer, otel_context as _otel_ctx, otel_trace as _otel_trace, StatusCode as _SC


# ── topological sort (Kahn's) ─────────────────────────────────────────────
def _topo(nodes, edges):
    ids   = [n['id'] for n in nodes]
    indeg = dict.fromkeys(ids, 0)
    succ  = dict.fromkeys(ids, [])
    for e in edges:
        s, t = e['source'], e['target']
        if s in succ and t in indeg:
            succ[s].append((t, e.get('sourceHandle')))
            indeg[t] += 1
    queue = [i for i in ids if indeg[i] == 0]
    order = []
    while queue:
        n = queue.pop(0)
        order.append(n)
        for (nb, _) in succ[n]:
            indeg[nb] -= 1
            if indeg[nb] == 0:
                queue.append(nb)
    return order, succ


def _subgraph_order(entry, subset, edges):
    sub_edges = [e for e in edges if e['source'] in subset and e['target'] in subset]
    sub_nodes = [{'id': i} for i in subset]
    order, succ = _topo(sub_nodes, sub_edges)
    if entry in order:
        order.remove(entry)
    return order, succ


def _reachable_via_handle(start_id: str, handle: str, succ: dict) -> set:
    """BFS from start_id's successors reached via `handle`."""
    frontier = {t for (t, h) in succ.get(start_id, []) if h == handle}
    visited  = set()
    queue    = list(frontier)
    while queue:
        n = queue.pop()
        if n not in visited:
            visited.add(n)
            for (t, _) in succ.get(n, []):
                if t not in visited:
                    queue.append(t)
    return visited


def _compute_levels(nodes, succ) -> list:
    """Group nodes into execution levels.

    All nodes within a level can run in parallel — their predecessors all
    belong to strictly earlier levels, so there are no read/write races on
    the shared context dict.
    """
    ids    = [n['id'] for n in nodes]
    indeg  = dict.fromkeys(ids, 0)
    id_set = set(ids)
    for nid in ids:
        for (t, _) in succ.get(nid, []):
            if t in id_set:
                indeg[t] += 1

    current = [i for i in ids if indeg[i] == 0]
    seen    = set(current)
    levels  = []

    while current:
        levels.append(list(current))
        nxt = []
        for nid in current:
            for (t, _) in succ.get(nid, []):
                if t not in seen:
                    indeg[t] -= 1
                    if indeg[t] == 0:
                        nxt.append(t)
                        seen.add(t)
        current = nxt

    return levels


# ── node dispatch ─────────────────────────────────────────────────────────
def _run_node(node_type, config, inp, context, logger, edges, nodes_map, creds=None, **kwargs):
    """Dispatch to the registered node handler."""
    from app.nodes import get_handler
    handler = get_handler(node_type)
    if handler is None:
        raise ValueError(f"Unknown node type: {node_type!r} — not found in node registry")
    nid = kwargs.get('_nid', '')
    # upstream_ids: list for backward-compat callers (action_merge, action_aggregate)
    upstream_ids = [e['source'] for e in edges if e['target'] == nid]
    # predecessor_ids: set for template injection guard in _render
    predecessor_ids = set(upstream_ids)
    return handler(
        config, inp, context, logger,
        creds=creds,
        upstream_ids=upstream_ids,
        edges=edges,
        nodes_map=nodes_map,
        predecessor_ids=predecessor_ids,
        **{k: v for k, v in kwargs.items() if k != '_nid'},
    )


def run_one_node(node: dict, inp: Any, context: dict,
                 creds: dict = None, logger=None,
                 edges: list = None, nodes_map: dict = None,
                 _depth: int = 0) -> dict:
    """Execute a single node directly and return a trace-compatible result dict.

    Used by the single-node test endpoint (/api/graphs/{id}/nodes/{nid}/test).
    Does not apply retry policy, skip checks, or loop expansion — pure invocation.

    Returns:
        {"output": ..., "duration_ms": ..., "error": None | str}
    """
    # Always wrap: run_graph wraps here, run_one_node wraps here. If called directly
    # (not via run_graph), we wrap. If called from run_graph, run_graph already
    # wrapped — but wrapping twice is safe since LoggingLoggerAdapter just delegates.
    logger = LoggingLoggerAdapter(logger if logger is not None else log)
    if creds is None:
        creds = {}
    if edges is None:
        edges = []
    if nodes_map is None:
        nodes_map = {}

    nid    = node.get('id', '')
    ntype  = node.get('type', node.get('data', {}).get('type', ''))
    config = node.get('data', {}).get('config', {})

    t_start = time.time()
    try:
        output = _run_node(
            ntype, config, inp, context, logger,
            edges, nodes_map, creds,
            _depth=_depth, _nid=nid,
        )
        return {
            "output":      output,
            "duration_ms": int((time.time() - t_start) * 1000),
            "error":       None,
        }
    except (JSONDecodeError, OSError, ValueError, TypeError, RuntimeError, AttributeError, ArithmeticError) as exc:
        return {
            "output":      None,
            "duration_ms": int((time.time() - t_start) * 1000),
            "error":       f"{type(exc).__name__}: {exc}",
        }


# ── per-node execution (used by both sequential and parallel paths) ────────
def _exec_node(nid, nodes_map, edges, context, creds, logger, succ, _depth):
    """Run one node and return (output, trace, skip_delta, loop_result).

    Never raises — errors are encoded in the trace and returned.
    loop_result is non-None only for action.loop nodes (caller handles expansion).
    skip_delta is a set of node IDs that should be skipped (from condition nodes).
    """
    node   = nodes_map.get(nid)
    if not node:
        return None, None, set(), None

    ntype  = node.get('type', '')
    ndata  = node.get('data', {})
    config = ndata.get('config', {})

    # ── OTEL: per-node span — child of the enclosing run_graph span ───────
    _n_span  = _get_tracer("hiverunr.executor").start_span(f"node.{ntype}")
    _n_span.set_attribute("node.id",    nid)
    _n_span.set_attribute("node.type",  ntype)
    _n_span.set_attribute("node.label", ndata.get('label', ''))
    _n_span.set_attribute("node.depth", _depth)
    _n_token = _otel_ctx.attach(_otel_trace.set_span_in_context(_n_span))

    try:
        # Collect input from the most-recently-connected upstream node.
        # Root nodes (no incoming edges) receive the initial payload.
        inp = {}
        is_root = True
        for e in edges:
            if e['target'] == nid:
                is_root = False
                if e['source'] in context:
                    inp = context[e['source']]
                break
        if is_root and '__initial_payload__' in context:
            inp = context['__initial_payload__']

        _inp_str    = json.dumps(inp, default=str) if isinstance(inp, (dict, list)) else str(inp)
        _inp_stored = inp if len(_inp_str) < 5_000_000 else {'__truncated': True, '__size': len(_inp_str)}

        trace = {
            'node_id':     nid,
            'type':        ntype,
            'label':       ndata.get('label', ''),
            'status':      'skipped',
            'duration_ms': 0,
            'attempts':    0,
            'input':       _inp_stored,
            'output':      None,
            'error':       None,
        }

        retry_max   = int(ndata.get('retry_max', 0))
        retry_delay = float(ndata.get('retry_delay', 5))
        fail_mode   = ndata.get('fail_mode', 'abort')

        t_start  = time.time()
        last_err = None
        result   = None

        for attempt in range(retry_max + 1):
            try:
                if attempt > 0:
                    log.info(f"RETRY {attempt}/{retry_max} {ntype} [{nid}]")
                    time.sleep(retry_delay)
                result = _run_node(
                    ntype, config, inp, context, logger,
                    edges, nodes_map, creds,
                    _depth=_depth, _nid=nid,
                )
                last_err = None
                break
            except (JSONDecodeError, OSError, ValueError, TypeError, RuntimeError, AttributeError, ArithmeticError) as e:
                last_err = e
                log.info(f"ERROR attempt {attempt+1} {ntype} [{nid}]: {e}")

        trace['duration_ms'] = int((time.time() - t_start) * 1000)
        trace['attempts']    = attempt + 1

        if last_err is not None:
            err_msg = f"{type(last_err).__name__}: {last_err}"
            trace['status'] = 'error'
            trace['error']  = err_msg
            _n_span.set_status(_SC.ERROR, err_msg)
            _n_span.record_exception(last_err)
            if fail_mode == 'continue':
                error_out = {'__error': err_msg, '__node': nid, '__type': ntype}
                trace['output'] = error_out
                log.info(f"CONTINUE-ON-ERROR [{nid}]: {err_msg}")
                return error_out, trace, set(), None
            else:
                result_err = {'__error': err_msg}
                return result_err, trace, set(), ('abort', last_err, nid, ntype, attempt + 1)

        _out_str = json.dumps(result, default=str) if isinstance(result, (dict, list)) else str(result)
        trace['status'] = 'ok'
        trace['output'] = result if len(_out_str) < 5_000_000 else {'__truncated': True, '__size': len(_out_str)}

        # Compute condition skip delta
        skip_delta = set()
        if ntype == 'action.condition':
            condition_val = result.get('result', False) if isinstance(result, dict) else bool(result)
            true_reach  = _reachable_via_handle(nid, 'true',  succ)
            false_reach = _reachable_via_handle(nid, 'false', succ)
            if condition_val:
                skip_delta = false_reach - true_reach
                log.info(f"Condition [{nid}] = True  → skipping {len(skip_delta)} false-branch node(s)")
            else:
                skip_delta = true_reach - false_reach
                log.info(f"Condition [{nid}] = False → skipping {len(skip_delta)} true-branch node(s)")

        # Detect loop node (caller handles body expansion sequentially)
        loop_result = result if (isinstance(result, dict) and result.get('__loop__')) else None

        return result, trace, skip_delta, loop_result

    finally:
        _n_span.set_attribute("node.attempts",    trace.get('attempts', 0) if 'trace' in dir() else 0)
        _n_span.set_attribute("node.duration_ms", trace.get('duration_ms', 0) if 'trace' in dir() else 0)
        _n_span.set_attribute("node.status",      trace.get('status', 'unknown') if 'trace' in dir() else 'unknown')
        _otel_ctx.detach(_n_token)
        _n_span.end()


# ── loop body expansion (sequential, called after any loop node completes) ─
def _expand_loop(nid, loop_result, nodes_map, edges, context, creds, logger, succ):
    """Run loop body for each item and store aggregated results in context."""
    items        = loop_result.get('items', [])
    body_targets = [t for (t, h) in succ.get(nid, []) if h == 'body']
    body_set     = set()
    for bt in body_targets:
        body_set.add(bt)
        for (nb, _) in succ.get(bt, []):
            body_set.add(nb)
    loop_results = []
    for item in items:
        body_order, _ = _subgraph_order(nid, body_set, edges)
        loop_ctx = {**context, nid: item, 'item': item}
        for bid in body_order:
            bn = nodes_map.get(bid)
            if not bn:
                continue
            b_inp = loop_ctx.get(bid, item)
            for e in edges:
                if e['target'] == bid and e['source'] in loop_ctx:
                    b_inp = loop_ctx[e['source']]
                    break
            try:
                from app.nodes import get_handler as _gh
                _h = _gh(bn.get('type', ''))
                if _h:
                    loop_ctx[bid] = _h(
                        bn.get('data', {}).get('config', {}),
                        b_inp, loop_ctx, logger, creds=creds,
                        upstream_ids=[], edges=edges, nodes_map=nodes_map,
                    )
                else:
                    loop_ctx[bid] = {'__error': f"Unknown node type in loop: {bn.get('type')}"}
            except (AttributeError, TypeError, KeyError, ValueError, ArithmeticError, OSError) as e:
                loop_ctx[bid] = {'__error': str(e)}
        loop_results.append(loop_ctx.get(body_targets[0]) if body_targets else item)
    return {'loop_results': loop_results, 'count': len(loop_results)}


# ── main graph runner ─────────────────────────────────────────────────────
def run_graph(graph_data: dict, initial_payload: dict = None, logger=None, _depth: int = 0,
              node_callback=None, workspace_id: int = None,
              start_node_id: str = None, prior_context: dict = None) -> dict:
    """Execute a graph and return {context, results, traces}.

    node_callback(event: dict) — optional callable fired after each node
    completes with a trace-compatible dict plus a 'type' key
    ('node_start' | 'node_done').  Safe to be None.
    """
    logger = LoggingLoggerAdapter(logger if logger is not None else log)
    if _depth > 5:
        raise RuntimeError("Call Graph: maximum sub-flow nesting depth (5) exceeded")

    nodes   = graph_data.get('nodes', [])
    edges   = graph_data.get('edges', [])
    payload = initial_payload or {}
    context = {}
    results = {}
    traces  = []

    try:
        from app.core.db import load_all_credentials
        creds = load_all_credentials(workspace_id=workspace_id)
    except (OSError, TimeoutError, ImportError) as e:
        log.warning(f"Could not load credentials: {e}")
        creds = {}
    except (ConnectionError, OSError, RuntimeError, TimeoutError) as exc:
        log.warning(f"Could not load credentials (infra): {exc}")
        creds = {}
    except JSONDecodeError:
        # Corrupt data in DB credential store — skip credentials for this run
        log.warning("Corrupt credential data in DB")
        creds = {}
    except psycopg2.Error as e:
        # psycopg2.OperationalError and subclasses (DB unavailable, connection refused)
        log.warning(f"Could not load credentials (DB): {e}")
        creds = {}
    except redis.exceptions.ConnectionError as e:
        # Redis unavailable — degrade gracefully instead of crashing the whole run
        log.warning(f"Could not load credentials (Redis): {e}")
        creds = {}
    except Exception as e:
        # Any other unexpected error that is not one of the above — still degrade
        # gracefully but log at error level so it shows up in monitoring
        log.error(f"Unexpected error loading credentials: {e}")
        creds = {}

    nodes_map    = {n['id']: n for n in nodes}
    order, succ  = _topo(nodes, edges)
    skip_nodes   = set()

    # Seed initial payload so root nodes receive it as their input
    context['__initial_payload__'] = payload.copy()

    # ── "Run from this node" support ─────────────────────────────────────────
    # When start_node_id is provided, pre-populate context with prior run
    # outputs and mark all ancestors as skipped so the executor resumes
    # from the requested node using previous outputs as inputs.
    if start_node_id and start_node_id in {n['id'] for n in nodes}:
        # Build the ancestor set (BFS backwards from start_node_id)
        pred = {n['id']: [] for n in nodes}
        for e in edges:
            if e['target'] in pred and e['source'] in pred:
                pred[e['target']].append(e['source'])
        visited = set()
        queue = [start_node_id]
        while queue:
            cur = queue.pop()
            for p in pred.get(cur, []):
                if p not in visited:
                    visited.add(p)
                    queue.append(p)
        # Skip all ancestors
        skip_nodes.update(visited)
        # Pre-populate context from prior run outputs (if provided)
        if prior_context:
            for node_id, output in prior_context.items():
                if node_id not in ('__initial_payload__',) and node_id in visited:
                    context[node_id] = output
        log.info(f"[run_from] starting at node {start_node_id}, skipping {len(skip_nodes)} ancestor(s)")

    # Determine execution mode
    parallel    = graph_data.get('parallel', False) or \
                  os.environ.get('EXECUTOR_PARALLEL', '').lower() == 'true'
    max_workers = int(os.environ.get('EXECUTOR_MAX_WORKERS', '8'))

    # ── OTEL: root span for this graph run ────────────────────────────────
    _tracer = _get_tracer("hiverunr.executor")
    _span   = _tracer.start_span("run_graph")
    _span.set_attribute("graph.node_count", len(nodes))
    _span.set_attribute("graph.edge_count", len(edges))
    _span.set_attribute("graph.depth",      _depth)
    _span.set_attribute("graph.parallel",   parallel)
    if workspace_id:
        _span.set_attribute("workspace.id", workspace_id)
    _token = _otel_ctx.attach(_otel_trace.set_span_in_context(_span))

    try:
        log.info("Graph starting: %s nodes, %s edges, parallel=%s, depth=%d",
                    len(nodes), len(edges), parallel, _depth)
        if parallel and max_workers > 1:
            levels = _compute_levels(nodes, succ)
            _run_parallel(levels, nodes_map, edges, context, results, traces,
                          creds, logger, succ, skip_nodes, _depth, max_workers,
                          node_callback=node_callback, start_node_id=start_node_id)
        else:
            _run_sequential(order, nodes_map, edges, context, results, traces,
                            creds, logger, succ, skip_nodes, _depth,
                            node_callback=node_callback, start_node_id=start_node_id)
        log.info("Graph complete: %s nodes executed, %s traces", len(results), len(traces))
    except (OSError, TimeoutError) as exc:
        _span.set_status(_SC.ERROR, str(exc))
        log.error("Graph failed: %s (%s)", type(exc).__name__, exc)
        raise
    except Exception as exc:
        _span.set_status(_SC.ERROR, str(exc))
        log.error("Graph failed: %s (%s)", type(exc).__name__, exc)
        raise
    finally:
        _span.set_attribute("graph.trace_count", len(traces))
        _otel_ctx.detach(_token)
        _span.end()

    return {'context': context, 'results': results, 'traces': traces}


# ── sequential execution path ─────────────────────────────────────────────
def _run_sequential(order, nodes_map, edges, context, results, traces,
                    creds, logger, succ, skip_nodes, _depth, node_callback=None,
                    start_node_id=None):
    for nid in order:
        node  = nodes_map.get(nid)
        if not node:
            continue
        ntype = node.get('type', '')
        ndata = node.get('data', {})

        # Collect input
        inp = {}
        for e in edges:
            if e['target'] == nid and e['source'] in context:
                inp = context[e['source']]
                break

        _inp_str    = json.dumps(inp, default=str) if isinstance(inp, (dict, list)) else str(inp)
        _inp_stored = inp if len(_inp_str) < 5_000_000 else {'__truncated': True, '__size': len(_inp_str)}

        trace = {
            'node_id': nid, 'type': ntype, 'label': ndata.get('label', ''),
            'status': 'skipped', 'duration_ms': 0, 'attempts': 0,
            'input': _inp_stored, 'output': None, 'error': None,
        }

        if ntype == 'note':
            results[nid] = {'__ui_only': True}
            traces.append(trace)
            continue

        if ndata.get('disabled', False) and nid != start_node_id:
            log.info(f"SKIP (disabled) {ntype} [{nid}]")
            results[nid] = {'__disabled': True}
            traces.append(trace)
            continue

        if nid in skip_nodes:
            log.info(f"SKIP (branch not taken) {ntype} [{nid}]")
            results[nid] = {'__skipped': True}
            trace['error'] = 'Branch not taken'
            traces.append(trace)
            continue

        context[nid] = inp  # pre-seed so downstream can read even if node errors

        # Fire node_start before execution
        if node_callback:
            try:
                node_callback({'type': 'node_start', 'node_id': nid,
                               'label': ndata.get('label', ''), 'node_type': ntype})
            except (AttributeError, TypeError) as exc:
                log.warning("node_callback(node_start) failed for %s: %s", nid, exc)

        result, trace, skip_delta, abort_or_loop = _exec_node(
            nid, nodes_map, edges, context, creds, logger, succ, _depth
        )

        skip_nodes.update(skip_delta)

        if isinstance(abort_or_loop, tuple) and abort_or_loop[0] == 'abort':
            _, exc, a_nid, a_ntype, attempts = abort_or_loop
            context[nid] = {'__error': str(exc)}
            results[nid] = {'__error': str(exc)}
            traces.append(trace)
            if node_callback:
                try:
                    node_callback({'type': 'node_done', **trace})
                except (AttributeError, TypeError) as exc:
                    log.warning("node_callback(node_done) failed for %s: %s", nid, exc)
            raise RuntimeError(
                f"Node [{a_nid}] ({a_ntype}) failed after {attempts} attempt(s): {exc}"
            ) from exc

        context[nid] = result
        results[nid] = result
        traces.append(trace)
        if node_callback:
            try:
                node_callback({'type': 'node_done', **trace})
            except (AttributeError, TypeError) as exc:
                log.warning("node_callback(node_done) failed for %s: %s", nid, exc)

        # Loop body expansion
        if abort_or_loop is not None and isinstance(abort_or_loop, dict) \
                and abort_or_loop.get('__loop__'):
            expanded = _expand_loop(nid, abort_or_loop, nodes_map, edges,
                                    context, creds, logger, succ)
            context[nid] = expanded
            results[nid] = expanded


# ── parallel execution path ───────────────────────────────────────────────
def _run_parallel(levels, nodes_map, edges, context, results, traces,
                  creds, logger, succ, skip_nodes, _depth, max_workers,
                  node_callback=None, start_node_id=None):
    """Level-based parallel execution.

    Nodes within the same level have no data dependencies between them —
    they only read context keys written by earlier levels — so they are
    safe to run concurrently.  Between levels, skip_nodes and loop expansion
    are processed sequentially.
    """
    for level in levels:
        # Filter: skip disabled, note, and branch-skipped nodes
        active = []
        for nid in level:
            node  = nodes_map.get(nid)
            if not node:
                continue
            ntype = node.get('type', '')
            ndata = node.get('data', {})

            if ntype == 'note':
                results[nid] = {'__ui_only': True}
                traces.append({'node_id': nid, 'type': ntype,
                                'label': ndata.get('label', ''),
                                'status': 'skipped', 'duration_ms': 0, 'attempts': 0,
                                'input': None, 'output': None, 'error': None})
                continue

            if ndata.get('disabled', False) and nid != start_node_id:
                log.info(f"SKIP (disabled) {ntype} [{nid}]")
                results[nid] = {'__disabled': True}
                traces.append({'node_id': nid, 'type': ntype,
                                'label': ndata.get('label', ''),
                                'status': 'skipped', 'duration_ms': 0, 'attempts': 0,
                                'input': None, 'output': None, 'error': None})
                continue

            if nid in skip_nodes:
                log.info(f"SKIP (branch not taken) {ntype} [{nid}]")
                results[nid] = {'__skipped': True}
                traces.append({'node_id': nid, 'type': ntype,
                                'label': ndata.get('label', ''),
                                'status': 'skipped', 'duration_ms': 0, 'attempts': 0,
                                'input': None, 'output': None, 'error': 'Branch not taken'})
                continue

            # Pre-seed context so downstream can read even if node errors
            for e in edges:
                if e['target'] == nid and e['source'] in context:
                    context[nid] = context[e['source']]
                    break
            active.append(nid)

        if not active:
            continue

        # Fire node_start for all active nodes before any execute
        if node_callback:
            for nid in active:
                nd = nodes_map.get(nid, {})
                try:
                    node_callback({'type': 'node_start', 'node_id': nid,
                                   'label': nd.get('data', {}).get('label', ''),
                                   'node_type': nd.get('type', '')})
                except (AttributeError, TypeError) as exc:
                    log.warning("node_callback(node_start) failed for level node %s: %s", nid, exc)

        if len(active) == 1:
            # Fast path: avoid threading overhead for single-node levels
            nid = active[0]
            result, trace, skip_delta, abort_or_loop = _exec_node(
                nid, nodes_map, edges, context, creds, logger, succ, _depth
            )
            _apply_node_result(nid, result, trace, skip_delta, abort_or_loop,
                               context, results, traces, skip_nodes,
                               nodes_map, edges, creds, logger, succ,
                               node_callback=node_callback)
        else:
            # Parallel: run all active nodes in this level concurrently
            node_results = {}
            workers = min(len(active), max_workers)
            with ThreadPoolExecutor(max_workers=workers) as pool:
                future_map = {
                    pool.submit(
                        _exec_node, nid, nodes_map, edges,
                        dict(context),  # snapshot — reads only, no races
                        creds, logger, succ, _depth
                    ): nid
                    for nid in active
                }
                for future in as_completed(future_map):
                    nid = future_map[future]
                    try:
                        node_results[nid] = future.result()
                    except (AttributeError, TypeError) as exc:
                        log.warning("_exec_node raised in parallel worker for %s: %s", nid, exc)
                        node_results[nid] = (
                            {'__error': str(exc)},
                            {'node_id': nid, 'status': 'error', 'error': str(exc),
                             'duration_ms': 0, 'attempts': 1, 'output': None, 'input': None,
                             'type': '', 'label': ''},
                            set(),
                            ('abort', exc, nid, '', 1),
                        )

            # Apply results in topological order of this level (preserves trace ordering)
            for nid in active:
                result, trace, skip_delta, abort_or_loop = node_results[nid]
                _apply_node_result(nid, result, trace, skip_delta, abort_or_loop,
                                   context, results, traces, skip_nodes,
                                   nodes_map, edges, creds, logger, succ,
                                   node_callback=node_callback)


def _apply_node_result(nid, result, trace, skip_delta, abort_or_loop,
                       context, results, traces, skip_nodes,
                       nodes_map, edges, creds, logger, succ, node_callback=None):
    """Apply a completed node's result to shared state. Called sequentially."""
    skip_nodes.update(skip_delta)

    if isinstance(abort_or_loop, tuple) and abort_or_loop[0] == 'abort':
        _, exc, a_nid, a_ntype, attempts = abort_or_loop
        context[nid] = {'__error': str(exc)}
        results[nid] = {'__error': str(exc)}
        traces.append(trace)
        if node_callback:
            try:
                node_callback({'type': 'node_done', **trace})
            except (AttributeError, TypeError) as exc:
                log.warning("node_callback(node_done) failed for %s: %s", nid, exc)
        raise RuntimeError(
            f"Node [{a_nid}] ({a_ntype}) failed after {attempts} attempt(s): {exc}"
        ) from exc

    context[nid] = result
    results[nid] = result
    traces.append(trace)
    if node_callback:
        try:
            node_callback({'type': 'node_done', **trace})
        except (AttributeError, TypeError) as exc:
            log.warning("node_callback(node_done) failed for %s: %s", nid, exc)

    if abort_or_loop is not None and isinstance(abort_or_loop, dict) \
            and abort_or_loop.get('__loop__'):
        expanded = _expand_loop(nid, abort_or_loop, nodes_map, edges,
                                context, creds, logger, succ)
        context[nid] = expanded
        results[nid] = expanded
