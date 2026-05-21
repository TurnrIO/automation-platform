import os, json, logging, io, sys, runpy
from pathlib import Path
from celery import Celery

# Load secrets before any env-var reads (Celery broker URL, AgentMail creds, etc.)
from app.core.secrets import load_secrets
load_secrets()
from app.telemetry import setup_tracing
setup_tracing()
from app.core.db import (
    update_run,
    get_graph
)

SCRIPTS_DIR = Path(__file__).parent / 'workflows'

log    = logging.getLogger(__name__)
# CELERY_BROKER_URL is the standard Celery env var; fall back to REDIS_URL so
# operators only need to set one variable in their .env file.
broker = os.environ.get("CELERY_BROKER_URL") or os.environ.get("REDIS_URL", "redis://redis:6379/0")
app    = Celery("hiverunr", broker=broker, backend=broker)
app.conf.task_serializer   = "json"
app.conf.result_serializer = "json"
app.conf.accept_content    = ["json"]


# ── Alert helpers ─────────────────────────────────────────────────────────────
def _fire_webhook(webhook_url: str, payload: dict) -> None:
    """POST alert payload to a webhook URL. Never raises."""
    try:
        import httpx
        httpx.post(webhook_url, json=payload, timeout=10)
    except (httpx.HTTPError, OSError) as exc:
        log.warning("webhook alert failed (%s): %s", webhook_url[:60], exc)


def _send_run_alert(
    *,
    graph_id: int | None,
    flow_name: str,
    status: str,
    task_id: str,
    error: str = "",
) -> None:
    """Dispatch email + webhook alerts for a completed graph run.

    Reads per-flow alert config from the DB; also honours the global
    OWNER_EMAIL env var for system-level failure notifications.

    NOTE: ALL alert sending must go through this function.
    Do not call app.email.send_* directly from routers or other tasks —
    it bypasses the per-flow enable/disable settings and the webhook dispatch.
    """
    from app.email import send_run_alert, _is_configured

    alert_emails:     str  = ""
    alert_webhook:    str  = ""
    alert_on_success: bool = False
    alert_min_failures: int = 1

    # Per-flow config
    if graph_id:
        try:
            from app.core.db import get_graph_alerts, count_trailing_failures
            cfg = get_graph_alerts(graph_id)
            if cfg:
                alert_emails       = cfg.get("alert_emails") or ""
                alert_webhook      = cfg.get("alert_webhook") or ""
                alert_on_success   = bool(cfg.get("alert_on_success", False))
                alert_min_failures = int(cfg.get("alert_min_failures") or 1)
        except (OSError, KeyError, ValueError, TypeError, RuntimeError) as exc:
            log.warning("Could not load alert config for graph %s: %s", graph_id, exc)

    # Decide whether to fire
    is_failure = status in ("failed", "dead")
    should_alert = is_failure or (alert_on_success and status == "succeeded")

    if not should_alert:
        return

    # Consecutive-failure threshold: only alert if the last N runs all failed
    if is_failure and alert_min_failures > 1 and graph_id:
        try:
            streak = count_trailing_failures(graph_id)
            if streak < alert_min_failures:
                log.debug(
                    "Alert suppressed for graph %s: streak=%d < min=%d",
                    graph_id, streak, alert_min_failures,
                )
                return
        except (AttributeError, TypeError, KeyError, ValueError) as exc:
            log.warning("Could not check failure streak for graph %s: %s", graph_id, exc)

    webhook_payload = {
        "event":     "run.failed" if is_failure else "run.succeeded",
        "flow":      flow_name,
        "graph_id":  graph_id,
        "task_id":   task_id,
        "status":    status,
        "error":     error or None,
    }

    # Webhook
    if alert_webhook:
        _fire_webhook(alert_webhook, webhook_payload)

    # Email — per-flow recipients
    if alert_emails and _is_configured():
        send_run_alert(
            to=alert_emails,
            flow_name=flow_name,
            status=status,
            task_id=task_id,
            error=error,
            graph_id=graph_id,
        )

    # Email — global owner alert on failure only
    owner_email = os.environ.get("OWNER_EMAIL", "")
    if is_failure and owner_email and _is_configured():
        # Only send to owner if not already included in per-flow list
        existing = {e.strip().lower() for e in alert_emails.split(",") if e.strip()}
        if owner_email.lower() not in existing:
            send_run_alert(
                to=owner_email,
                flow_name=flow_name,
                status=status,
                task_id=task_id,
                error=error,
                graph_id=graph_id,
            )


def _notify_failure(name: str, error: str, task_id: str, graph_id: int = None):
    """Legacy wrapper — kept for backward compatibility with enqueue_script."""
    _send_run_alert(
        graph_id=graph_id,
        flow_name=name,
        status="failed",
        task_id=task_id,
        error=error,
    )



@app.task(bind=True, name="app.worker.enqueue_workflow")
def enqueue_workflow(self, workflow_name: str, payload: dict):
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except (OSError, ImportError):
        pass
    update_run(task_id, "running")
    try:
        from app.workflows import example
        workflows = {"example": example.run}
        if workflow_name not in workflows:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        result = workflows[workflow_name](payload)
        update_run(task_id, "succeeded", result=result)
    except (ValueError, TypeError, RuntimeError) as e:
        # Permanent failures — fail immediately without retry classification
        log.error(f"Workflow {workflow_name} failed permanently: {e}")
        update_run(task_id, "dead", result={"error": str(e)})
        _notify_failure(workflow_name, str(e), task_id)
    except (OSError, RuntimeError, TypeError, KeyError, AttributeError, ValueError) as exc:
        # Transient failures — logged by outer retry handler in Celery task
        raise exc

@app.task(bind=True, name="app.worker.enqueue_script")
def enqueue_script(self, script_name: str, payload: dict):
    """Execute a standalone Python script from the workflows directory."""
    import time as _time
    log.info(f"enqueue_script started: script={script_name} payload_keys={list(payload.keys())}")
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except (OSError, ImportError):
        pass
    buf = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
    t_start = _time.time()
    try:
        # Mark running first — inside try so any DB error is caught too
        update_run(task_id, "running")
        script_path = SCRIPTS_DIR / f"{script_name}.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_name}.py")
        sys.stdout = sys.stderr = buf
        runpy.run_path(str(script_path), run_name="__main__",
                       init_globals={"__payload__": payload})
        sys.stdout, sys.stderr = old_stdout, old_stderr
        output = buf.getvalue()
        duration_ms = int((_time.time() - t_start) * 1000)
        traces = [{
            'node_id':     'script',
            'type':        'script',
            'label':       script_name,
            'status':      'ok',
            'duration_ms': duration_ms,
            'attempts':    1,
            'input':       payload,
            'output':      output or "(no output)",
            'error':       None,
        }]
        update_run(task_id, "succeeded", result={"output": output, "script": script_name},
                   traces=traces)
    except Exception as e:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        # BaseException subclasses that should propagate, not be logged as errors:
        # - SystemExit: sys.exit(0) means "script completed successfully" — Celery handles via task lifecycle
        # - KeyboardInterrupt: SIGINT / Ctrl+C — worker shutdown signal
        if isinstance(e, (SystemExit, KeyboardInterrupt)):
            if isinstance(e, SystemExit):
                log.info(f"Script {script_name} exited with code={e.code or 0}")
            raise
        log.exception(f"Script {script_name} failed")
        _notify_failure(script_name, str(e), task_id)
        output = buf.getvalue()
        duration_ms = int((_time.time() - t_start) * 1000)
        traces = [{
            'node_id':     'script',
            'type':        'script',
            'label':       script_name,
            'status':      'error',
            'duration_ms': duration_ms,
            'attempts':    1,
            'input':       payload,
            'output':      output or None,
            'error':       f"{type(e).__name__}: {e}",
        }]
        try:
            update_run(task_id, "failed",
                       result={"error": str(e), "script": script_name, "output": output},
                       traces=traces)
        except (OSError, KeyError, ValueError, RuntimeError):
            pass  # best-effort — don't let a DB error create a second FAILURE


def _make_run_publisher(task_id: str):
    """Return a publish(event_dict) callable that pushes to Redis pub/sub.

    The channel name is  run:<task_id>:stream  — the same key the SSE
    endpoint subscribes to.  Returns None (silently) if Redis is unavailable.
    """
    try:
        import redis as _redis
        r = _redis.from_url(broker, socket_connect_timeout=2, socket_timeout=2)
        r.ping()
        channel = f"run:{task_id}:stream"

        def _publish(event: dict):
            try:
                r.publish(channel, json.dumps(event, default=str))
            except (OSError, RuntimeError):
                pass  # non-fatal — fall back to DB polling on the client

        return _publish
    except (OSError, ConnectionError, TimeoutError):
        return None


# Exceptions that indicate a transient infrastructure problem worth retrying.
# Flow logic errors (ValueError, KeyError, etc.) are NOT retried — they are
# permanent failures that the user needs to fix in the flow definition.
_TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    OSError,
)

# Maximum Celery retries before a run is marked "dead" (permanently failed).
_MAX_RETRIES = 3
# Exponential backoff: 30 s, 60 s, 120 s between retry attempts.
_RETRY_BACKOFF_BASE = 30  # seconds


@app.task(bind=True, name="app.worker.enqueue_graph", max_retries=_MAX_RETRIES)
def enqueue_graph(self, graph_id: int, payload: dict,
                  start_node_id: str = None, prior_context: dict = None):
    """Execute a graph flow.

    Retry policy: up to 3 retries with exponential backoff (30 s / 60 s / 120 s)
    for transient infrastructure errors (connection failures, timeouts, OS errors).
    Flow logic errors (bad config, missing node, etc.) fail immediately — they
    require the user to fix the flow, not just wait and try again.

    Run status lifecycle:
      queued → running → succeeded   (happy path)
      queued → running → retrying    (transient error, will retry)
      queued → running → dead        (exhausted retries or permanent error)
    """
    task_id = self.request.id
    retry_attempt = self.request.retries  # 0 on first attempt, 1+ on retries
    try:
        from app.core.db import init_db
        init_db()
    except (OSError, ImportError):
        pass
    update_run(task_id, "running", retry_count=retry_attempt)
    traces = []
    g = None

    publish = _make_run_publisher(task_id)

    # ── OTEL: root span for this Celery task ──────────────────────────────
    from app.telemetry import get_tracer as _get_tracer, otel_context as _otel_ctx, otel_trace as _otel_trace, StatusCode as _SC
    _tracer  = _get_tracer("hiverunr.worker")
    _w_span  = _tracer.start_span("graph.run")
    _w_span.set_attribute("celery.task_id", task_id)
    _w_span.set_attribute("graph.id",       graph_id)
    _w_span.set_attribute("celery.attempt", retry_attempt)
    _w_token = _otel_ctx.attach(_otel_trace.set_span_in_context(_w_span))

    try:
        g = get_graph(graph_id)
        if not g:
            raise ValueError(f"Graph {graph_id} not found")
        _w_span.set_attribute("graph.name",         g.get("name", ""))
        _w_span.set_attribute("graph.workspace_id", g.get("workspace_id") or 0)
        graph_data = json.loads(g.get('graph_json') or '{}')
        from app.core.executor import run_graph

        def _streaming_logger(msg: str):
            if publish:
                publish({"type": "log", "msg": msg})

        def _node_callback(event: dict):
            if publish:
                publish(event)

        result = run_graph(graph_data, payload,
                           logger=_streaming_logger,
                           node_callback=_node_callback,
                           workspace_id=g.get('workspace_id'),
                           start_node_id=start_node_id,
                           prior_context=prior_context)
        traces = result.get('traces', [])
        update_run(task_id, "succeeded", result=result, traces=traces, retry_count=retry_attempt)
        if publish:
            publish({"type": "run_done", "status": "succeeded"})
        _send_run_alert(
            graph_id=graph_id,
            flow_name=g.get("name", f"graph#{graph_id}"),
            status="succeeded",
            task_id=task_id,
        )

    except _TRANSIENT_EXCEPTIONS as exc:
        # Transient error — retry with exponential backoff if attempts remain.
        attempt = self.request.retries
        countdown = _RETRY_BACKOFF_BASE * (2 ** attempt)  # 30, 60, 120 s
        log.warning(
            "Graph %s transient error (attempt %d/%d, retry in %ds): %s",
            graph_id, attempt + 1, _MAX_RETRIES, countdown, exc,
        )
        update_run(
            task_id, "retrying",
            result={"error": str(exc), "retry_attempt": attempt + 1, "retry_in_seconds": countdown},
            traces=traces,
            retry_count=attempt + 1,
        )
        if publish:
            publish({"type": "run_done", "status": "retrying",
                     "error": str(exc), "retry_attempt": attempt + 1})
        _w_span.set_status(_SC.ERROR, str(exc))
        _w_span.set_attribute("celery.will_retry", True)
        raise self.retry(exc=exc, countdown=countdown)

    except Exception as exc:
        # Permanent failure — log, mark dead, send alert.
        log.exception("Graph %s failed permanently (attempt %d)", graph_id, retry_attempt + 1)
        final_status = "dead" if retry_attempt >= _MAX_RETRIES else "failed"
        flow_name = g.get("name", f"graph#{graph_id}") if g else f"graph#{graph_id}"
        update_run(
            task_id, final_status,
            result={"error": str(exc)},
            traces=traces,
            retry_count=retry_attempt,
        )
        if publish:
            publish({"type": "run_done", "status": final_status, "error": str(exc)})
        _send_run_alert(
            graph_id=graph_id,
            flow_name=flow_name,
            status="failed",
            task_id=task_id,
            error=str(exc),
        )
        _w_span.set_status(_SC.ERROR, str(exc))

    finally:
        _otel_ctx.detach(_w_token)
        _w_span.end()
