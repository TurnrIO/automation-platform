"""Schedules router."""
from json import JSONDecodeError
import json as _json
import logging
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Optional

from app.deps import _check_admin, _resolve_workspace
from app.core.db import list_schedules, create_schedule, update_schedule, toggle_schedule, delete_schedule, get_schedule, update_run

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/schedules")
def api_schedules(request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    return list_schedules(workspace_id=workspace_id)


class ScheduleCreate(BaseModel):
    name: str
    graph_id: Optional[int] = None
    workflow: Optional[str] = None
    cron: Optional[str] = None
    payload: dict = {}
    timezone: str = "UTC"
    run_at: Optional[str] = None   # ISO datetime string for one-shot schedules


class ScheduleUpdate(BaseModel):
    name: str
    graph_id: Optional[int] = None
    workflow: Optional[str] = None
    cron: Optional[str] = None
    payload: dict = {}
    timezone: str = "UTC"
    run_at: Optional[str] = None


@router.post("/api/schedules")
def api_create_schedule(body: ScheduleCreate, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    return create_schedule(
        body.name, body.workflow, body.graph_id,
        body.cron, body.payload, body.timezone, body.run_at,
        workspace_id=workspace_id,
    )


@router.put("/api/schedules/{sid}")
def api_update_schedule(sid: int, body: ScheduleUpdate, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    row = update_schedule(
        sid, body.name, body.workflow, body.graph_id,
        body.cron, body.payload, body.timezone, body.run_at,
        workspace_id=workspace_id
    )
    if not row:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return row


@router.post("/api/schedules/{sid}/toggle")
def api_toggle_schedule(sid: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT workspace_id FROM schedules WHERE id=%s", (sid,))
            row = cur.fetchone()
    except (AttributeError, RuntimeError):
        row = None
    if not row:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    result = toggle_schedule(sid)
    if not result:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"id": sid}



@router.delete("/api/schedules/{sid}")
def api_delete_schedule(sid: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    from app.core.db import get_conn
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT workspace_id FROM schedules WHERE id=%s", (sid,))
            row = cur.fetchone()
    except (AttributeError, RuntimeError):
        row = None
    if not row:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if workspace_id is not None and row.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    delete_schedule(sid)
    return {"deleted": True}


@router.post("/api/schedules/{sid}/run-now")
def api_run_schedule_now(sid: int, request: Request):
    """"Manually trigger a schedule immediately, using its stored payload."""
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    s = get_schedule(sid)
    if not s:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if workspace_id is not None and s.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if not s.get("graph_id"):
        raise HTTPException(status_code=400, detail="Schedule has no graph_id — cannot trigger manually")

    from app.core.executor import run_graph
    from app.worker import enqueue_graph
    from app.core.db import get_conn, get_graph, init_db
    import uuid
    import psycopg2.extras
    payload = s.get("payload") or {}
    if isinstance(payload, str):
        try:
            payload = _json.loads(payload)
        except JSONDecodeError:
            payload = {}

    graph_id = s["graph_id"]
    task_id = None
    try:
        task = enqueue_graph.apply_async(args=[graph_id, payload])
        task_id = task.id
    except (ConnectionError, OSError, RuntimeError) as exc:
        log.warning("Celery unavailable (%s) — running schedule %s inline", exc, sid)
        task_id = str(uuid.uuid4())
        try:
            with get_conn() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id) "
                    "VALUES(%s,%s,'queued',%s,%s)",
                    (task_id, graph_id, _json.dumps(payload), s.get("workspace_id"))
                )
                conn.commit()
        except psycopg2.Error as db_exc:
            log.warning("Could not pre-create run record for schedule %s: %s", sid, db_exc)

        try:
            init_db()
            g = get_graph(graph_id)
            if not g:
                raise RuntimeError(f"Graph {graph_id} not found")
            try:
                graph_data = _json.loads(g.get('graph_json') or '{}')
            except JSONDecodeError:
                graph_data = {}
            result = run_graph(
                graph_data,
                payload,
                workspace_id=g.get('workspace_id'),
                log=log,
            )
            update_run(task_id, "succeeded", result=result,
                       traces=result.get('traces', []))
        except (ValueError, TypeError, AttributeError, OSError, RuntimeError) as run_exc:
            log.exception("Inline schedule run failed for schedule %s", sid)
            update_run(task_id, "failed", result={"error": str(run_exc)})

    # Pre-create run record if Celery was available
    if task is not None:
        try:
            with get_conn() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id) "
                    "VALUES(%s,%s,'queued',%s,%s)",
                    (task.id, graph_id, _json.dumps(payload), s.get("workspace_id"))
                )
                conn.commit()
        except psycopg2.Error as exc:
            log.warning("Could not pre-create run record for schedule %s: %s", sid, exc)

    log.info("Manual trigger: schedule %s → graph %s (task %s)", sid, graph_id, task_id)
    return {"queued": True, "task_id": task_id, "graph_id": graph_id}


@router.get("/api/schedules/next-run")
def api_cron_next_run(
    cron:     str = Query(..., description="5-part cron expression"),
    timezone: str = Query("UTC", description="IANA timezone name"),
    count:    int = Query(5, ge=1, le=20, description="How many future dates to return"),
):
    """Validate a cron expression and return the next N fire times."""
    try:
        from apscheduler.triggers.cron import CronTrigger
        import datetime as _dt
        import pytz

        try:
            tz = pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            return {"valid": False, "error": f"Unknown timezone: {timezone}"}

        trigger = CronTrigger.from_crontab(cron, timezone=tz)
        now = _dt.datetime.now(_dt.timezone.utc)
        next_times = []
        t = now
        for _ in range(count):
            t = trigger.get_next_fire_time(None, t)
            if t is None:
                break
            next_times.append(t.isoformat())
            t = t + _dt.timedelta(seconds=1)

        return {"valid": True, "next": next_times}

    except (ValueError, TypeError) as exc:
        return {"valid": False, "error": str(exc)}
