"""Runs router — list, delete, trim, replay, stream."""
import json
from json import JSONDecodeError
import logging
import os
import time as _time
import redis as _redis
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional

from app.deps import _check_admin, _require_run_scope, _require_manage_scope, _resolve_workspace
from app.core.db import (
    list_runs, delete_run, clear_runs, bulk_delete_runs, get_run_by_task, update_run, get_graph,
    trim_runs_by_count, trim_runs_by_age,
    get_retention_policy, set_retention_policy,
    get_ratelimit_policy, set_ratelimit_policy,
    set_run_note,
    decode_json_value,
    log_audit,
)
from app.core.executor import run_graph

log = logging.getLogger(__name__)
router = APIRouter()


def _sync_stuck_runs():
    """Reconcile queued/running runs against the Celery result backend."""
    try:
        from app.core.db import get_conn
        import psycopg2.extras
        from celery.result import AsyncResult
        from app.worker import app as _celery_app
        with get_conn() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT task_id, workflow, status,
                       EXTRACT(EPOCH FROM (NOW() - created_at)) AS age_seconds
                FROM runs
                WHERE status IN ('queued','running')
                  AND created_at < NOW() - INTERVAL '5 seconds'
            """)
            stuck = cur.fetchall()
        for row in stuck:
            try:
                res   = AsyncResult(row['task_id'], app=_celery_app)
                state = res.state
                age   = float(row['age_seconds'] or 0)
                if state == 'SUCCESS':
                    update_run(row['task_id'], 'succeeded',
                               result=res.result if isinstance(res.result, dict) else {'output': str(res.result)})
                elif state == 'FAILURE':
                    err = str(res.result) if res.result else 'Task failed (check worker logs)'
                    update_run(row['task_id'], 'failed', result={'error': err})
                elif state == 'REVOKED':
                    update_run(row['task_id'], 'cancelled', result={'cancelled_by': 'celery_revoke'})
                elif state == 'PENDING':
                    if age > 120:
                        log.info("[_sync_stuck_runs] Task %s auto-killed after 120s in PENDING state (workflow=%s, age=%.0fs)",
                                 row['task_id'], row.get('workflow'), age)
                        update_run(row['task_id'], 'failed',
                                   result={'error': 'Task was lost — worker may have been restarting. Please re-run.'})
                    elif row['workflow']:
                        from app.worker import enqueue_script as _enqueue_script
                        _enqueue_script.apply_async(args=[row['workflow'], {}], task_id=row['task_id'])
                        log.info(f"Re-dispatched lost task {row['task_id']} for {row['workflow']}")
            except (AttributeError, TypeError, RuntimeError, KeyError) as exc:
                log.warning("[_sync_stuck_runs] Celery state lookup failed for task %s: %s", row['task_id'], exc)
    except (AttributeError, TypeError, KeyError, RuntimeError, OSError) as exc:
        log.warning("[_sync_stuck_runs] Stuck-run reconciliation failed: %s", exc)


@router.get("/api/runs")
def api_runs(
    request: Request,
    page:      int            = Query(1,   ge=1,   description="Page number (1-based)"),
    page_size: int            = Query(50,  ge=1, le=200, description="Rows per page"),
    status:    Optional[str]  = Query(None, description="Filter by status"),
    flow_id:   Optional[int]  = Query(None, description="Filter by graph_workflows.id"),
    q:         Optional[str]  = Query(None, description="Search flow name / task_id"),
):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    _sync_stuck_runs()
    return list_runs(page=page, page_size=page_size, status=status, flow_id=flow_id, q=q, workspace_id=workspace_id)


@router.get("/api/runs/by-task/{task_id}")
def api_run_by_task(task_id: str, request: Request):
    """Lightweight single-run polling endpoint — used by canvas during execution."""
    _check_admin(request)
    run = get_run_by_task(task_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run


@router.delete("/api/runs/{run_id}")
def api_delete_run(run_id: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    import psycopg2.extras
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT workspace_id FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Run belongs to a different workspace")
    delete_run(run_id)
    log_audit(user["username"], "run.delete", "run", run_id, None,
              request.client.host if request.client else None)
    return {"deleted": True}


@router.delete("/api/runs")
def api_clear_runs(request: Request):
    user = _require_manage_scope(request)
    workspace_id = _resolve_workspace(request, user)
    clear_runs(workspace_id=workspace_id)
    log_audit(user["username"], "run.clear_all", None, None,
              {"workspace_id": workspace_id},
              request.client.host if request.client else None)
    return {"cleared": True}


class BulkDeleteBody(BaseModel):
    ids: list[int]


@router.post("/api/runs/bulk-delete")
def api_bulk_delete_runs(body: BulkDeleteBody, request: Request):
    """Delete a specific set of runs by ID. Returns the count actually deleted."""
    user = _require_manage_scope(request)
    if not body.ids:
        return {"deleted": 0}
    workspace_id = _resolve_workspace(request, user)
    deleted = bulk_delete_runs(body.ids, workspace_id=workspace_id)
    log_audit(user["username"], "run.bulk_delete", None, None,
              {"count": deleted, "ids": body.ids[:20]},  # log first 20 IDs max
              request.client.host if request.client else None)
    return {"deleted": deleted}


class TrimRunsBody(BaseModel):
    keep: int = 100
    days: Optional[int] = None   # if set, trim by age instead of count


@router.post("/api/runs/trim")
def api_trim_runs(body: TrimRunsBody, request: Request):
    """Trim run history.

    - `keep` (default 100): keep the N most recent runs, delete the rest.
    - `days`: delete runs older than N days (takes precedence over `keep` when set).
    """
    user = _require_manage_scope(request)
    workspace_id = _resolve_workspace(request, user)
    if body.days is not None:
        deleted = trim_runs_by_age(body.days, workspace_id=workspace_id)
        log_audit(user["username"], "run.trim", None, None,
                  {"mode": "age", "older_than_days": body.days, "deleted": deleted, "workspace_id": workspace_id},
                  request.client.host if request.client else None)
        return {"deleted": deleted, "mode": "age", "older_than_days": body.days}
    else:
        deleted = trim_runs_by_count(body.keep, workspace_id=workspace_id)
        log_audit(user["username"], "run.trim", None, None,
                  {"mode": "count", "kept": body.keep, "deleted": deleted, "workspace_id": workspace_id},
                  request.client.host if request.client else None)
        return {"deleted": deleted, "mode": "count", "kept": body.keep}


# ── Retention policy settings ─────────────────────────────────────────────────
@router.get("/api/runs/retention")
def api_get_retention(request: Request):
    _require_manage_scope(request)
    return get_retention_policy()


class RetentionBody(BaseModel):
    enabled: bool = False
    mode:    str  = "count"   # "count" | "age"
    count:   int  = 500
    days:    int  = 30


@router.put("/api/runs/retention")
def api_set_retention(body: RetentionBody, request: Request):
    user = _require_manage_scope(request)
    if body.mode not in ("count", "age"):
        raise HTTPException(422, "mode must be 'count' or 'age'")
    if body.count < 1:
        raise HTTPException(422, "count must be ≥ 1")
    if body.days < 1:
        raise HTTPException(422, "days must be ≥ 1")
    set_retention_policy(body.enabled, body.mode, body.count, body.days)
    log_audit(user["username"], "settings.retention", None, None,
              {"enabled": body.enabled, "mode": body.mode,
               "count": body.count, "days": body.days},
              request.client.host if request.client else None)
    return get_retention_policy()


# ── Rate-limit settings ───────────────────────────────────────────────────────
@router.get("/api/settings/ratelimit")
def api_get_ratelimit(request: Request):
    _require_manage_scope(request)
    policy = get_ratelimit_policy()
    # Attach live per-token counters from Redis
    counters = []
    try:
        r = _redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"))
        keys = r.keys("wh_rate:*")
        for k in sorted(keys)[:50]:  # cap at 50 entries
            token = k.split(":", 1)[1] if ":" in k else k
            count = int(r.get(k) or 0)
            ttl   = r.ttl(k)
            counters.append({"token": token, "count": count, "ttl_seconds": ttl})
    except (AttributeError, KeyError, OSError, RuntimeError) as exc:
        log.warning("Redis unavailable for rate-limit counters: %s", exc)
    except _redis.exceptions.RedisError as exc:
        log.warning("Redis unavailable for rate-limit counters: %s", exc)
    return {**policy, "counters": counters}


class RatelimitBody(BaseModel):
    limit:  int = 60   # calls per window; 0 = disabled
    window: int = 60   # window size in seconds


@router.put("/api/settings/ratelimit")
def api_set_ratelimit(body: RatelimitBody, request: Request):
    user = _require_manage_scope(request)
    if body.limit < 0:
        raise HTTPException(422, "limit must be ≥ 0 (use 0 to disable)")
    if body.window < 1:
        raise HTTPException(422, "window must be ≥ 1 second")
    set_ratelimit_policy(body.limit, body.window)
    log_audit(user["username"], "settings.ratelimit", None, None,
              {"limit": body.limit, "window": body.window},
              request.client.host if request.client else None)
    return get_ratelimit_policy()


@router.post("/api/runs/{run_id}/cancel")
def api_cancel_run(run_id: int, request: Request):
    """Revoke a queued or running Celery task and mark it cancelled.

    Uses terminate=True so an already-executing task is sent SIGTERM and
    stopped immediately.  Safe to call on a task that has already finished —
    Celery silently ignores the revoke in that case, and we only update the
    DB row if the run is still in a cancellable state.
    """
    user = _require_run_scope(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    import psycopg2.extras
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT task_id, status, workspace_id FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Run belongs to a different workspace")
    if row["status"] not in ("queued", "running"):
        raise HTTPException(400, f"Run is already {row['status']} — cannot cancel")
    task_id = row["task_id"]
    try:
        from celery.result import AsyncResult
        from app.worker import app as _celery_app
        AsyncResult(task_id, app=_celery_app).revoke(terminate=True, signal="SIGTERM")
    except (AttributeError, RuntimeError, OSError) as exc:
        log.warning("Could not revoke Celery task %s: %s", task_id, exc)
    update_run(task_id, "cancelled", result={"cancelled_by": "user"})
    log_audit(user["username"], "run.cancel", "run", run_id,
              {"task_id": task_id},
              request.client.host if request.client else None)
    return {"cancelled": True, "run_id": run_id, "task_id": task_id}


class _NoteBody(BaseModel):
    note: Optional[str] = None  # pass null / empty to clear the note


@router.put("/api/runs/{run_id}/note")
def api_set_run_note(run_id: int, body: _NoteBody, request: Request):
    """Add or update a freeform text note on a run. Pass null/empty to clear."""
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    import psycopg2.extras
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT workspace_id FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Run belongs to a different workspace")
    set_run_note(run_id, body.note)
    return {"ok": True, "run_id": run_id, "note": body.note or None}


@router.get("/api/runs/queue")
def api_run_queue(request: Request):
    """Return current Celery queue depth and active worker counts.

    Uses Celery inspect() with a short timeout so it never blocks long.
    Falls back gracefully if the broker is unreachable.
    """
    _require_run_scope(request)
    try:
        from app.worker import app as _celery
        i = _celery.control.inspect(timeout=2)

        active_map    = i.active()    or {}
        reserved_map  = i.reserved()  or {}
        scheduled_map = i.scheduled() or {}

        active_count    = sum(len(v) for v in active_map.values())
        reserved_count  = sum(len(v) for v in reserved_map.values())
        scheduled_count = sum(len(v) for v in scheduled_map.values())
        worker_names    = sorted(set(list(active_map) + list(reserved_map) + list(scheduled_map)))

        # Per-worker summary
        workers = []
        for w in worker_names:
            workers.append({
                "name":      w,
                "active":    len(active_map.get(w,    [])),
                "reserved":  len(reserved_map.get(w,  [])),
                "scheduled": len(scheduled_map.get(w, [])),
            })

        return {
            "ok":           True,
            "active":       active_count,
            "reserved":     reserved_count,
            "scheduled":    scheduled_count,
            "total_queued": reserved_count + scheduled_count,
            "workers":      workers,
            "worker_count": len(workers),
        }
    except (AttributeError, RuntimeError, TypeError) as exc:
        log.warning("Queue inspect failed: %s", exc)
        return {"ok": False, "error": str(exc), "active": 0, "reserved": 0, "scheduled": 0, "total_queued": 0, "workers": [], "worker_count": 0}


class _ReplayBody(BaseModel):
    payload: Optional[dict] = None  # if provided, overrides stored initial_payload


@router.get("/api/runs/{run_id}/payload")
def api_get_run_payload(run_id: int, request: Request):
    """Return the initial_payload stored for a run (used to pre-fill replay modal)."""
    user = _require_run_scope(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    import psycopg2.extras
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT initial_payload, workspace_id FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Run belongs to a different workspace")
    payload = decode_json_value(row["initial_payload"], {})
    return {"run_id": run_id, "payload": payload}


@router.post("/api/runs/{run_id}/replay")
def api_replay_run(run_id: int, request: Request, body: _ReplayBody = None):
    """Re-enqueue a past run.

    If body.payload is provided it overrides the stored initial_payload,
    allowing callers to tweak the trigger data before re-running.
    """
    user = _require_run_scope(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    import psycopg2.extras
    from app.worker import enqueue_graph
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT graph_id, initial_payload, workspace_id FROM runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Run {run_id} not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Run belongs to a different workspace")
    graph_id = row["graph_id"]
    initial_payload = row["initial_payload"]
    if not graph_id:
        raise HTTPException(400, "Run is not associated with a graph (script run?)")
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, f"Graph {graph_id} not found")

    # Payload resolution: caller override > stored initial_payload > {}
    if body and body.payload is not None:
        payload = body.payload
    else:
        payload = decode_json_value(initial_payload, {})

    task_id = None
    try:
        task = enqueue_graph.apply_async(
            args=[graph_id, payload],
            priority=g.get("priority", 5),
        )
        task_id = task.id
    except (OSError, RuntimeError, AttributeError) as exc:
        log.warning("Celery unavailable (%s) — replaying graph inline", exc)
        import uuid
        task_id = str(uuid.uuid4())
        try:
            from app.core.db import init_db as _init_db
            _init_db()
            update_run(task_id, "running")
            try:
                graph_data = json.loads(g.get('graph_json') or '{}')
            except JSONDecodeError:
                graph_data = {}
            result = run_graph(
                graph_data,
                payload,
                workspace_id=g.get('workspace_id'),
            )
            update_run(task_id, "succeeded", result=result,
                       traces=result.get('traces', []))
        except (OSError, RuntimeError) as inline_err:
            log.exception("Inline graph replay failed")
            update_run(task_id, "failed", result={"error": str(inline_err)})
            raise HTTPException(500, f"Graph replay failed: {inline_err}")

    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id) VALUES(%s,%s,'queued',%s,%s)",
            (task_id, graph_id, json.dumps(payload), workspace_id)
        )
    log_audit(user["username"], "run.replay", "graph", graph_id,
              {"replayed_run_id": run_id, "task_id": task_id, "graph": g["name"],
               "payload_overridden": body is not None and body.payload is not None},
              request.client.host if request.client else None)
    return {"queued": True, "task_id": task_id, "graph": g["name"], "replayed_run_id": run_id}


# ── Real-time run log streaming (SSE) ─────────────────────────────────────────
@router.get("/api/runs/{task_id}/stream")
def api_stream_run(task_id: str, request: Request):
    """Server-Sent Events stream for a graph run.

    Emits JSON event objects:
      {"type": "node_start", "node_id": ..., "label": ..., "node_type": ...}
      {"type": "node_done",  "node_id": ..., "status": ..., "duration_ms": ..., ...trace fields}
      {"type": "log",        "msg": ...}
      {"type": "run_done",   "status": "succeeded"|"failed", "error"?: ...}

    If the run is already finished when the client connects, replays
    node_done events from stored traces then fires run_done immediately.
    Falls back to DB polling if Redis pub/sub is unavailable.
    """
    _check_admin(request)

    REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    TERMINAL  = {"succeeded", "failed", "cancelled"}

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, default=str)}\n\n"

    def event_gen():
        # ── Fast path: run already finished ───────────────────────────────
        run = get_run_by_task(task_id)
        if run and run.get("status") in TERMINAL:
            traces = run.get("traces") or []
            for t in traces:
                yield _sse({"type": "node_done", **t})
            yield _sse({"type": "run_done", "status": run["status"],
                        "error": (run.get("result") or {}).get("error")})
            return

        # ── Subscribe to Redis pub/sub ─────────────────────────────────────
        pubsub = None
        try:
            import redis as _redis
            r = _redis.from_url(REDIS_URL, socket_connect_timeout=2,
                                socket_timeout=2)
            r.ping()
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(f"run:{task_id}:stream")
        except (AttributeError, OSError, RuntimeError):
            pubsub = None  # fall through to polling-only path

        deadline       = _time.time() + 300   # 5-minute hard timeout
        last_db_check  = _time.time()
        last_heartbeat = _time.time()

        try:
            while _time.time() < deadline:
                # Redis path
                if pubsub:
                    msg = pubsub.get_message(timeout=0.3)
                    if msg:
                        raw = msg["data"]
                        if isinstance(raw, bytes):
                            raw = raw.decode()
                        try:
                            event = json.loads(raw)
                        except JSONDecodeError:
                            continue
                        yield f"data: {raw}\n\n"
                        if event.get("type") == "run_done":
                            return
                        last_db_check = _time.time()
                        continue

                # Heartbeat every 15 s (SSE comment keeps proxy alive)
                if _time.time() - last_heartbeat > 15:
                    yield ": ping\n\n"
                    last_heartbeat = _time.time()

                # DB fallback check every 3 s (catches Redis pub/sub misses)
                if _time.time() - last_db_check > 3:
                    last_db_check = _time.time()
                    run = get_run_by_task(task_id)
                    if run and run.get("status") in TERMINAL:
                        # Run finished but pub/sub event was missed — replay
                        traces = run.get("traces") or []
                        for t in traces:
                            yield _sse({"type": "node_done", **t})
                        yield _sse({"type": "run_done", "status": run["status"],
                                    "error": (run.get("result") or {}).get("error")})
                        return

                # No Redis and no completion yet — short sleep to avoid busy-spin
                if not pubsub:
                    _time.sleep(0.5)

        finally:
            if pubsub:
                try:
                    pubsub.unsubscribe()
                    pubsub.close()
                except (AttributeError, RuntimeError, OSError):
                    pass

        # Timeout — send a synthetic done event so the client doesn't hang
        yield _sse({"type": "run_done", "status": "timeout",
                    "error": "Stream timed out after 5 minutes"})

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx/Caddy response buffering
            "Connection": "keep-alive",
        },
    )
