"""HiveRunr scheduler — cron-driven workflow dispatcher.

HA / leader-election design
───────────────────────────
Multiple scheduler replicas can run simultaneously; only the one that
holds the Redis leader lock will actually fire jobs.  The others sit in
standby and take over within one lock TTL if the leader crashes or is
restarted.

Lock mechanics (Redis SET NX PX):
  • Lock key:      hiverunr:scheduler:leader
  • TTL:           SCHEDULER_LOCK_TTL_MS  (default 45 000 ms = 45 s)
  • Refresh every: SCHEDULER_LOCK_REFRESH_S (default 15 s)
  • Standby poll:  SCHEDULER_STANDBY_POLL_S (default 10 s)

Each replica generates a unique token on startup and uses that token as
the lock value.  Only the process whose token matches the stored value is
allowed to refresh or release the lock — preventing a slow process from
accidentally releasing a lock already taken over by a new leader.

Failover latency ≤ lock TTL (45 s by default).  For tighter failover,
lower SCHEDULER_LOCK_TTL_MS but keep refresh_period < TTL/2 to avoid
accidental expiry under normal load.

Graceful fallback: if Redis is unreachable at startup the scheduler runs
in single-instance mode (no HA, same behaviour as before this change).
"""
import os
import time
import logging
import psycopg2
import json
import secrets as _secrets

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

# Load secrets before DB connection (DATABASE_URL may come from provider)
from app.core.secrets import load_secrets
load_secrets()
from app.telemetry import setup_tracing
setup_tracing()

from app.core.db import init_db, list_schedules, delete_schedule, purge_expired_sessions, update_run
from app.core.executor import run_graph

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ── tuning knobs ──────────────────────────────────────────────────────────────
_LOCK_KEY       = "hiverunr:scheduler:leader"
_LOCK_TTL_MS    = int(os.environ.get("SCHEDULER_LOCK_TTL_MS",    "45000"))  # 45 s
_REFRESH_S      = int(os.environ.get("SCHEDULER_LOCK_REFRESH_S", "15"))     # refresh every 15 s
_STANDBY_POLL_S = int(os.environ.get("SCHEDULER_STANDBY_POLL_S", "10"))     # standby check every 10 s
_INSTANCE_ID    = _secrets.token_hex(8)   # unique identity for this process


def _redis_client():
    """Return a Redis client using REDIS_URL (same broker URL as Celery)."""
    import redis as _redis
    url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    return _redis.from_url(url, socket_connect_timeout=5, socket_timeout=5)


def _try_acquire(r) -> bool:
    """Attempt to acquire the leader lock. Returns True if this instance won."""
    result = r.set(_LOCK_KEY, _INSTANCE_ID, px=_LOCK_TTL_MS, nx=True)
    return result is True


# Lua script: extend TTL only if this instance still owns the lock.
_REFRESH_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua script: delete the key only if this instance still owns it.
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


def _try_refresh(r) -> bool:
    """Extend the lock TTL atomically. Returns False if lock was lost."""
    result = r.eval(_REFRESH_SCRIPT, 1, _LOCK_KEY, _INSTANCE_ID, _LOCK_TTL_MS)
    return bool(result)


def _release(r):
    """Release the lock atomically (no-op if we no longer own it)."""
    r.eval(_RELEASE_SCRIPT, 1, _LOCK_KEY, _INSTANCE_ID)


# ── job factory ───────────────────────────────────────────────────────────────
def _make_job(sched, scheduler_ref=None):
    sid = sched["id"]
    is_one_shot = bool(sched.get("run_at"))

    def job():
        from app.worker import enqueue_workflow, enqueue_graph, enqueue_script
        import json as _json
        payload = (
            _json.loads(sched["payload"])
            if isinstance(sched["payload"], str)
            else (sched["payload"] or {})
        )
        if sched.get("graph_id"):
            # Look up flow priority; fall back to 5 if column absent (pre-migration)
            try:
                from app.core.db import get_graph as _get_graph
                _g = _get_graph(sched["graph_id"])
                _priority = int((_g or {}).get("priority", 5))
            except (OSError, RuntimeError):
                _priority = 5

            task_id = None
            try:
                task = enqueue_graph.apply_async(args=[sched["graph_id"], payload], priority=_priority)
                task_id = task.id
            except (OSError, RuntimeError) as exc:
                log.warning("Celery unavailable (%s) — running scheduled graph inline", exc)
                import uuid
                task_id = str(uuid.uuid4())
                # Pre-create a run record so the DB has a 'queued' entry before
                # the inline executor transitions it to 'running'.
                try:
                    from app.core.db import get_conn
                    ws_id = sched.get("workspace_id")
                    with get_conn() as conn:
                        conn.cursor().execute(
                            "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id)"
                            " VALUES(%s,%s,'queued',%s,%s)"
                            " ON CONFLICT (task_id) DO NOTHING",
                            (task_id, sched["graph_id"], _json.dumps(payload), ws_id),
                        )
                except (ConnectionError, OSError, RuntimeError) as exc:
                    log.warning("Could not pre-create run record for inline schedule %s: %s", sid, exc)
                # Set running state before execution; catch infrastructure errors only
                try:
                    update_run(task_id, "running")
                except (ConnectionError, OSError, RuntimeError, psycopg2.Error):
                    log.warning("Could not update run %s to 'running' — skipping inline execution", task_id)
                    return
                try:
                    _g_data = json.loads(_g.get('graph_json') or '{}') if _g else {}
                    result = run_graph(_g_data, payload, workspace_id=sched.get("workspace_id"))
                    update_run(task_id, "succeeded", result=result,
                               traces=result.get('traces', []))
                except (OSError, RuntimeError, psycopg2.Error) as inline_err:
                    # Only catch infrastructure errors; flow logic errors (ValueError,
                    # TypeError, AttributeError from nodes) should propagate uncaught so
                    # the error is surfaced as a traceback rather than silently swallowed.
                    log.exception("Inline scheduled graph run failed")
                    update_run(task_id, "failed", result={"error": str(inline_err)})
                    return  # graph failed inline

            # Pre-create a run record scoped to the schedule's workspace
            if task_id:
                try:
                    from app.core.db import get_conn
                    workspace_id = sched.get("workspace_id")
                    with get_conn() as conn:
                        conn.cursor().execute(
                            "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id)"
                            " VALUES(%s,%s,'queued',%s,%s)"
                            " ON CONFLICT (task_id) DO NOTHING",
                            (task_id, sched["graph_id"], _json.dumps(payload), workspace_id),
                        )
                except (ConnectionError, OSError, RuntimeError) as exc:
                    log.warning("Could not pre-create run record for schedule %s: %s", sid, exc)
        elif sched.get("workflow", "").startswith("script:"):
            script_name = sched["workflow"][len("script:"):]
            enqueue_script.delay(script_name, payload)
        elif sched.get("workflow"):
            enqueue_workflow.delay(sched["workflow"], payload)
        # Auto-delete one-shot schedules after they fire
        if is_one_shot:
            try:
                delete_schedule(sid)
                log.info("One-shot schedule %s fired and removed", sid)
            except (AttributeError, KeyError, RuntimeError) as exc:
                log.warning("Could not auto-delete one-shot schedule %s: %s", sid, exc)
    return job


# ── schedule sync ─────────────────────────────────────────────────────────────
def _sync_schedules(scheduler, known: dict):
    """Add/remove APScheduler jobs to match the enabled rows in the DB."""
    schedules = list_schedules()
    current_ids = {s["id"] for s in schedules if s["enabled"]}

    # Remove stale jobs
    for sid in list(known):
        if sid not in current_ids:
            try:
                scheduler.remove_job(str(sid))
            except (AttributeError, KeyError, RuntimeError):
                pass
            del known[sid]

    # Add new jobs
    for s in schedules:
        if not s["enabled"]:
            continue
        sid = s["id"]
        if sid not in known:
            try:
                run_at = s.get("run_at")
                if run_at:
                    # One-shot: fire at the specified datetime then auto-delete
                    import datetime as _dt
                    if isinstance(run_at, str):
                        run_at_dt = _dt.datetime.fromisoformat(run_at.replace("Z", "+00:00"))
                    else:
                        run_at_dt = run_at
                    trigger = DateTrigger(run_date=run_at_dt)
                    log.info("Scheduled (once): %s at %s", s["name"], run_at)
                else:
                    trigger = CronTrigger.from_crontab(s["cron"], timezone=s.get("timezone", "UTC"))
                    log.info("Scheduled: %s (%s)", s["name"], s["cron"])
                scheduler.add_job(
                    _make_job(s),
                    trigger,
                    id=str(sid),
                    replace_existing=True,
                )
                known[sid] = s
            except (RuntimeError, OSError) as e:
                log.error("Failed to schedule %s: %s", s["name"], e)


# ── leader execution ──────────────────────────────────────────────────────────
def _purge_sessions():
    """Nightly maintenance job — remove sessions past their expires_at."""
    try:
        purge_expired_sessions()
        log.info("Session purge complete")
    except (AttributeError, OSError) as exc:
        log.warning("Session purge failed: %s", exc)


def _auto_trim_runs():
    """Nightly job — apply the configured run retention policy."""
    try:
        from app.core.db import get_retention_policy, trim_runs_by_count, trim_runs_by_age
        policy = get_retention_policy()
        if not policy["enabled"]:
            log.info("Auto-trim: retention policy disabled — skipping")
            return
        if policy["mode"] == "age":
            deleted = trim_runs_by_age(policy["days"])
            log.info("Auto-trim: deleted %d runs older than %d days", deleted, policy["days"])
        else:
            deleted = trim_runs_by_count(policy["count"])
            log.info("Auto-trim: deleted %d runs, kept %d most recent", deleted, policy["count"])
    except (AttributeError, OSError) as exc:
        log.warning("Auto-trim failed: %s", exc)


def _run_as_leader(r):
    """Run APScheduler for as long as we hold the lock.

    On each refresh tick we both extend the lock TTL and re-sync the
    schedule table, so changes made in the UI take effect within
    SCHEDULER_LOCK_REFRESH_S seconds.
    """
    log.info("Scheduler [%s] became LEADER — starting job execution", _INSTANCE_ID)
    scheduler = BlockingScheduler()
    known: dict = {}

    def refresh_and_sync():
        if not _try_refresh(r):
            log.warning(
                "Scheduler [%s] lost the leader lock — yielding to new leader",
                _INSTANCE_ID,
            )
            scheduler.shutdown(wait=False)
            return
        _sync_schedules(scheduler, known)

    _sync_schedules(scheduler, known)
    scheduler.add_job(
        refresh_and_sync, "interval", seconds=_REFRESH_S, id="__leader_refresh__"
    )
    # Nightly maintenance: purge sessions that have passed their expires_at
    scheduler.add_job(
        _purge_sessions, CronTrigger(hour=2, minute=0), id="__session_purge__"
    )
    # Nightly maintenance: auto-trim run history per retention policy
    scheduler.add_job(
        _auto_trim_runs, CronTrigger(hour=3, minute=0), id="__auto_trim__"
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        _release(r)
        log.info("Scheduler [%s] released leader lock", _INSTANCE_ID)


# ── fallback: no Redis ─────────────────────────────────────────────────────────
def _run_standalone():
    """Single-instance fallback when Redis is unavailable."""
    log.info("Scheduler [%s] running in standalone mode (no HA)", _INSTANCE_ID)
    scheduler = BlockingScheduler()
    known: dict = {}

    def refresh():
        _sync_schedules(scheduler, known)

    _sync_schedules(scheduler, known)
    scheduler.add_job(refresh, "interval", seconds=30, id="__refresh__")
    scheduler.add_job(
        _purge_sessions, CronTrigger(hour=2, minute=0), id="__session_purge__"
    )
    scheduler.add_job(
        _auto_trim_runs, CronTrigger(hour=3, minute=0), id="__auto_trim__"
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


# ── entry point ───────────────────────────────────────────────────────────────
def main():
    init_db()
    log.info(
        "Scheduler [%s] starting (lock_ttl=%dms refresh=%ds standby_poll=%ds)",
        _INSTANCE_ID, _LOCK_TTL_MS, _REFRESH_S, _STANDBY_POLL_S,
    )

    try:
        r = _redis_client()
        r.ping()
    except (OSError, RuntimeError) as exc:
        log.warning(
            "Redis unavailable (%s) — running in standalone mode (no HA)", exc
        )
        _run_standalone()
        return

    while True:
        if _try_acquire(r):
            _run_as_leader(r)
            # Lost the lock or was shut down — wait before trying again
            time.sleep(_STANDBY_POLL_S)
        else:
            owner = r.get(_LOCK_KEY)
            owner_str = owner.decode() if owner else "unknown"
            log.debug(
                "Scheduler [%s] standby — current leader: %s", _INSTANCE_ID, owner_str
            )
            time.sleep(_STANDBY_POLL_S)


if __name__ == "__main__":
    main()
