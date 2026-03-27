"""Graph workflow executor — v11
Architecture: pure orchestration. All node logic lives in app/nodes/.
New in v11:
  - Node registry dispatch (app/nodes/)
  - continue-on-error per node (fail_mode: "abort" | "continue")
  - Merge/Join node with upstream_ids injection
  - _render moved to app/nodes/_utils (executor re-exports for compat)
"""
import re, json, time, logging
from typing import Any

log = logging.getLogger(__name__)

# ── re-export _render for backward compatibility (call_graph etc) ──────────
from app.nodes._utils import _render  # noqa: F401

# ── topological sort (Kahn's) ─────────────────────────────────────────────
def _topo(nodes, edges):
    ids   = [n['id'] for n in nodes]
    indeg = {i: 0 for i in ids}
    succ  = {i: [] for i in ids}
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

# ── node dispatch ─────────────────────────────────────────────────────────
def _run_node(node_type, config, inp, context, logger, edges, nodes_map, creds=None, **kwargs):
    """Dispatch to the registered node handler."""
    from app.nodes import get_handler
    handler = get_handler(node_type)
    if handler is None:
        raise ValueError(f"Unknown node type: {node_type!r} — not found in node registry")

    # Build upstream_ids for merge node (and any future fan-in nodes)
    upstream_ids = [e['source'] for e in edges if e['target'] == kwargs.get('_nid', '')]

    return handler(
        config, inp, context, logger,
        creds=creds,
        upstream_ids=upstream_ids,
        edges=edges,
        nodes_map=nodes_map,
        **{k: v for k, v in kwargs.items() if k != '_nid'},
    )

# ── main graph runner ─────────────────────────────────────────────────────
def run_graph(graph_data: dict, initial_payload: dict = None, logger=None, _depth: int = 0) -> dict:
    if logger is None:
        logger = lambda msg: log.info(msg)
    if _depth > 5:
        raise RuntimeError("Call Graph: maximum sub-flow nesting depth (5) exceeded")

    nodes   = graph_data.get('nodes', [])
    edges   = graph_data.get('edges', [])
    payload = initial_payload or {}
    context = {}
    results = {}
    traces  = []

    # Load credentials once for this run
    try:
        from app.core.db import load_all_credentials
        creds = load_all_credentials()
    except Exception as e:
        log.warning(f"Could not load credentials: {e}")
        creds = {}

    nodes_map  = {n['id']: n for n in nodes}
    order, succ = _topo(nodes, edges)
    skip_nodes  = set()   # nodes on un-taken condition branches

    for nid in order:
        node  = nodes_map.get(nid)
        if not node:
            continue
        ntype  = node.get('type', '')
        ndata  = node.get('data', {})
        config = ndata.get('config', {})

        # Collect input: prefer the most-recently-connected upstream node
        inp = payload.copy()
        for e in edges:
            if e['target'] == nid and e['source'] in context:
                inp = context[e['source']]
                break
        context[nid] = inp

        # Store input in trace (capped at 10000 chars)
        _inp_str    = json.dumps(inp, default=str) if isinstance(inp, (dict, list)) else str(inp)
        _inp_stored = inp if len(_inp_str) < 10000 else {'__truncated': True, '__size': len(_inp_str)}

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

        # ── skip disabled nodes ───────────────────────────────────────
        if ndata.get('disabled', False):
            logger(f"SKIP (disabled) {ntype} [{nid}]")
            results[nid] = {'__disabled': True}
            trace['status'] = 'skipped'
            traces.append(trace)
            continue

        # ── skip nodes on un-taken condition branch ───────────────────
        if nid in skip_nodes:
            logger(f"SKIP (branch not taken) {ntype} [{nid}]")
            results[nid] = {'__skipped': True}
            trace['status'] = 'skipped'
            trace['error']  = 'Branch not taken'
            traces.append(trace)
            continue

        # ── retry + fail_mode policy ──────────────────────────────────
        retry_max   = int(ndata.get('retry_max', 0))
        retry_delay = float(ndata.get('retry_delay', 5))
        # fail_mode: "abort" raises and stops the graph (default, v10 behaviour)
        #            "continue" stores the error in context and keeps going
        fail_mode   = ndata.get('fail_mode', 'abort')

        t_start  = time.time()
        last_err = None
        result   = None

        for attempt in range(retry_max + 1):
            try:
                if attempt > 0:
                    logger(f"RETRY {attempt}/{retry_max} {ntype} [{nid}]")
                    time.sleep(retry_delay)
                result = _run_node(
                    ntype, config, inp, context, logger,
                    edges, nodes_map, creds,
                    _depth=_depth, _nid=nid,
                )
                last_err = None
                break
            except Exception as e:
                last_err = e
                logger(f"ERROR attempt {attempt+1} {ntype} [{nid}]: {e}")

        t_end = time.time()
        trace['duration_ms'] = int((t_end - t_start) * 1000)
        trace['attempts']    = attempt + 1

        if last_err is not None:
            err_msg = f"{type(last_err).__name__}: {last_err}"
            trace['status'] = 'error'
            trace['error']  = err_msg

            if fail_mode == 'continue':
                # Store error in context so downstream nodes can inspect it
                error_out = {'__error': err_msg, '__node': nid, '__type': ntype}
                context[nid] = error_out
                results[nid] = error_out
                trace['output'] = error_out
                traces.append(trace)
                logger(f"CONTINUE-ON-ERROR [{nid}]: {err_msg}")
                continue
            else:
                results[nid] = {'__error': err_msg}
                traces.append(trace)
                raise RuntimeError(
                    f"Node [{nid}] ({ntype}) failed after {attempt+1} attempt(s): {last_err}"
                ) from last_err

        context[nid] = result
        results[nid] = result
        trace['status'] = 'ok'
        _out_str = json.dumps(result, default=str) if isinstance(result, (dict, list)) else str(result)
        trace['output'] = result if len(_out_str) < 10000 else {'__truncated': True, '__size': len(_out_str)}
        traces.append(trace)

        # ── condition branching ───────────────────────────────────────
        if ntype == 'action.condition':
            condition_val = result.get('result', False) if isinstance(result, dict) else bool(result)
            true_reach  = _reachable_via_handle(nid, 'true',  succ)
            false_reach = _reachable_via_handle(nid, 'false', succ)
            if condition_val:
                skip_nodes |= (false_reach - true_reach)
                logger(f"Condition [{nid}] = True  → skipping {len(false_reach - true_reach)} false-branch node(s)")
            else:
                skip_nodes |= (true_reach - false_reach)
                logger(f"Condition [{nid}] = False → skipping {len(true_reach - false_reach)} true-branch node(s)")

        # ── loop body expansion ───────────────────────────────────────
        if isinstance(result, dict) and result.get('__loop__'):
            items        = result.get('items', [])
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
                    if not bn: continue
                    b_inp = loop_ctx.get(bid, item)
                    for e in edges:
                        if e['target'] == bid and e['source'] in loop_ctx:
                            b_inp = loop_ctx[e['source']]; break
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
                    except Exception as e:
                        loop_ctx[bid] = {'__error': str(e)}
                loop_results.append(loop_ctx.get(body_targets[0]) if body_targets else item)
            context[nid] = {'loop_results': loop_results, 'count': len(loop_results)}
            results[nid] = context[nid]

    return {'context': context, 'results': results, 'traces': traces}

