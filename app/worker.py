import os, json, logging, io, sys, runpy, smtplib
from pathlib import Path
from email.mime.text import MIMEText
from celery import Celery
from app.core.db import (
    get_run_by_task, update_run,
    list_workflows, get_graph
)

SCRIPTS_DIR = Path(__file__).parent / 'workflows'

log    = logging.getLogger(__name__)
broker = os.environ.get("CELERY_BROKER_URL", "redis://redis:6379/0")
app    = Celery("automations", broker=broker, backend=broker)
app.conf.task_serializer   = "json"
app.conf.result_serializer = "json"
app.conf.accept_content    = ["json"]


# ── failure notifications ─────────────────────────────────────────────────
def _notify_failure(name: str, error: str, task_id: str):
    """Send failure alerts via NOTIFY_SLACK_WEBHOOK and/or NOTIFY_EMAIL env vars."""
    slack_url    = os.environ.get("NOTIFY_SLACK_WEBHOOK", "")
    notify_email = os.environ.get("NOTIFY_EMAIL", "")
    short_id     = task_id[:8] if task_id else "?"
    short_err    = str(error)[:300]

    if slack_url:
        try:
            import httpx
            httpx.post(slack_url, json={
                "text": f":x: *{name}* failed (task `{short_id}`)\n```{short_err}```"
            }, timeout=10)
        except Exception as ex:
            log.warning(f"Slack notify failed: {ex}")

    if notify_email:
        try:
            smtp_host = os.environ.get("SMTP_HOST", "")
            smtp_user = os.environ.get("SMTP_USER", "")
            smtp_pass = os.environ.get("SMTP_PASS", "")
            smtp_port = int(os.environ.get("SMTP_PORT", 465))
            if not smtp_host:
                log.warning("NOTIFY_EMAIL set but SMTP_HOST not configured")
                return
            msg = MIMEText(
                f"Task ID:  {task_id}\nWorkflow: {name}\n\nError:\n{error}"
            )
            msg["Subject"] = f"[HiveRunr] {name} failed"
            msg["From"]    = smtp_user or "hiverunr@noreply.local"
            msg["To"]      = notify_email
            with smtplib.SMTP_SSL(smtp_host, smtp_port) as s:
                if smtp_user and smtp_pass:
                    s.login(smtp_user, smtp_pass)
                s.sendmail(smtp_user, notify_email, msg.as_string())
        except Exception as ex:
            log.warning(f"Email notify failed: {ex}")

@app.task(bind=True, name="app.worker.enqueue_workflow")
def enqueue_workflow(self, workflow_name: str, payload: dict):
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    update_run(task_id, "running")
    try:
        from app.workflows import example
        workflows = {"example": example.run}
        if workflow_name not in workflows:
            raise ValueError(f"Unknown workflow: {workflow_name}")
        result = workflows[workflow_name](payload)
        update_run(task_id, "succeeded", result=result)
    except Exception as e:
        log.exception(f"Workflow {workflow_name} failed")
        update_run(task_id, "failed", result={"error": str(e)})
        _notify_failure(workflow_name, str(e), task_id)

@app.task(bind=True, name="app.worker.enqueue_script")
def enqueue_script(self, script_name: str, payload: dict):
    """Execute a standalone Python script from the workflows directory."""
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    buf = io.StringIO()
    old_stdout, old_stderr = sys.stdout, sys.stderr
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
        update_run(task_id, "succeeded", result={"output": output, "script": script_name})
    except Exception as e:
        sys.stdout, sys.stderr = old_stdout, old_stderr
        log.exception(f"Script {script_name} failed")
        _notify_failure(script_name, str(e), task_id)
        try:
            update_run(task_id, "failed",
                       result={"error": str(e), "script": script_name, "output": buf.getvalue()})
        except Exception:
            pass  # best-effort — don't let a DB error create a second FAILURE


@app.task(bind=True, name="app.worker.enqueue_graph")
def enqueue_graph(self, graph_id: int, payload: dict):
    task_id = self.request.id
    try:
        from app.core.db import init_db
        init_db()
    except Exception:
        pass
    update_run(task_id, "running")
    traces = []
    try:
        g = get_graph(graph_id)
        if not g:
            raise ValueError(f"Graph {graph_id} not found")
        graph_data = json.loads(g.get('graph_json') or '{}')
        from app.core.executor import run_graph
        msgs = []
        result = run_graph(graph_data, payload, logger=msgs.append)
        traces = result.get('traces', [])
        update_run(task_id, "succeeded", result=result, traces=traces)
    except Exception as e:
        log.exception(f"Graph {graph_id} failed")
        update_run(task_id, "failed", result={"error": str(e)}, traces=traces)
        _notify_failure(f"graph#{graph_id}", str(e), task_id)
