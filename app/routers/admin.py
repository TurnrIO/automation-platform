"""Admin, maintenance, system status, metrics, scripts, nodes, and templates routers."""
import json
import httpx
import logging
from json import JSONDecodeError
import re as _re
import uuid
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query, Request
from typing import Optional

from app.deps import _check_admin, _require_owner
from app.core.db import log_audit, get_audit_log
from app._version import __version__

log = logging.getLogger(__name__)

SCRIPTS_DIR   = Path(__file__).parent.parent / 'workflows'
TEMPLATES_DIR = Path(__file__).parent.parent / 'templates'
MAX_SCRIPT_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB max script content

router = APIRouter()


# ── Prometheus metrics scrape endpoint ───────────────────────────────────────
# Moved from /metrics → /api/prometheus to avoid conflicting with the admin
# SPA page at /metrics (React Router route served by FastAPI catch-all).
# Update your prometheus.yml scrape_configs target accordingly.
@router.get("/api/prometheus", include_in_schema=False)
@router.get("/api/prometheus/metrics", include_in_schema=False)
def prometheus_metrics(request: Request):
    """Prometheus text exposition — auth-gated so metrics aren't public."""
    _check_admin(request)
    from app.observability import metrics_response
    return metrics_response()



# ── System diagnostics ───────────────────────────────────────────────────────
@router.get("/api/system/status")
def api_system_status(request: Request):
    """Full system diagnostics for the System page.

    Each subsystem returns:
      status  : "ok" | "warning" | "error"
      message : human-readable one-liner (always present)
      fix     : what to do if status != "ok" (present when relevant)
      + subsystem-specific fields
    """
    import os, sys, platform, socket, time
    _check_admin(request)
    results: dict = {}

    # ── Database ──────────────────────────────────────────────────────────────
    try:
        from app.core.db import get_conn, get_pool_stats
        t0 = time.monotonic()
        with get_conn() as conn:
            cur = conn.cursor()
            # Use aliases so fetchone() works with both plain and RealDictCursor
            cur.execute(
                "SELECT "
                "  (SELECT COUNT(*) FROM runs)            AS run_count,"
                "  (SELECT COUNT(*) FROM graph_workflows) AS flow_count,"
                "  pg_size_pretty(pg_database_size(current_database())) AS db_size"
            )
            row = cur.fetchone()
            run_count  = row["run_count"]  if isinstance(row, dict) else row[0]
            flow_count = row["flow_count"] if isinstance(row, dict) else row[1]
            db_size    = row["db_size"]    if isinstance(row, dict) else row[2]
            # Check Alembic migration head
            cur.execute("SELECT version_num FROM alembic_version")
            migration_row = cur.fetchone()
            migration_current = (migration_row["version_num"] if isinstance(migration_row, dict)
                                 else migration_row[0]) if migration_row else "unknown"
        latency_ms = round((time.monotonic() - t0) * 1000)
        pool = get_pool_stats()
        pool_msg = (
            f"pool {pool['pool_in_use']}/{pool['pool_max']} in use"
            if pool else ""
        )
        results["db"] = {
            "status": "ok",
            "message": f"Connected · {run_count} runs · {flow_count} flows · {db_size}"
                       + (f" · {pool_msg}" if pool_msg else ""),
            "run_count": run_count,
            "flow_count": flow_count,
            "db_size": db_size,
            "migration": migration_current,
            "latency_ms": latency_ms,
            "pool": pool,
        }
    except (OSError, RuntimeError, AttributeError, KeyError) as exc:
        results["db"] = {
            "status": "error",
            "message": str(exc),
            "fix": "Check DATABASE_URL in .env and ensure the postgres container is running.",
        }

    # ── Redis ─────────────────────────────────────────────────────────────────
    try:
        import redis as _redis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        t0 = time.monotonic()
        r = _redis.from_url(redis_url, socket_connect_timeout=2)
        r.ping()
        latency_ms = round((time.monotonic() - t0) * 1000)
        display_url = redis_url.split("@")[-1]  # strip credentials if present
        results["redis"] = {
            "status": "ok",
            "message": f"Reachable at {display_url}",
            "latency_ms": latency_ms,
        }
    except (OSError, RuntimeError, AttributeError) as exc:
        results["redis"] = {
            "status": "error",
            "message": str(exc),
            "fix": "Check REDIS_URL in .env and ensure the redis container is running.",
        }

    # ── Celery workers ────────────────────────────────────────────────────────
    try:
        from app.worker import app as celery_app
        pong = celery_app.control.ping(timeout=2)
        worker_names = [list(w.keys())[0] for w in pong]
        count = len(worker_names)
        if count == 0:
            results["worker"] = {
                "status": "warning",
                "message": "No workers responding",
                "fix": "Run `docker compose up -d worker` or check worker container logs.",
                "workers": 0,
                "worker_names": [],
            }
        else:
            results["worker"] = {
                "status": "ok",
                "message": f"{count} worker{'s' if count != 1 else ''} online",
                "workers": count,
                "worker_names": worker_names,
            }
    except (OSError, RuntimeError, AttributeError) as exc:
        results["worker"] = {
            "status": "error",
            "message": str(exc),
            "fix": "Check REDIS_URL (Celery broker) and restart the worker container.",
        }

    # ── Scheduler ─────────────────────────────────────────────────────────────
    try:
        import redis as _redis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = _redis.from_url(redis_url, socket_connect_timeout=2)
        lock_val = r.get("hiverunr:scheduler:leader")
        if lock_val:
            results["scheduler"] = {
                "status": "ok",
                "message": f"Leader lock held by instance {lock_val.decode()[:16]}…",
                "lock_held": True,
            }
        else:
            results["scheduler"] = {
                "status": "warning",
                "message": "Scheduler leader lock not found — scheduler may not be running",
                "fix": "Check that the scheduler container/process is running: `docker compose ps scheduler`.",
                "lock_held": False,
            }
    except (OSError, RuntimeError, AttributeError) as exc:
        results["scheduler"] = {
            "status": "warning",
            "message": f"Could not check scheduler lock: {exc}",
            "lock_held": False,
        }

    # ── Email (AgentMail) ─────────────────────────────────────────────────────
    api_key  = os.environ.get("AGENTMAIL_API_KEY", "").strip()
    from_addr = os.environ.get("AGENTMAIL_FROM", "").strip()
    owner_email = os.environ.get("OWNER_EMAIL", "").strip()
    if api_key and from_addr and owner_email:
        results["email"] = {
            "status": "ok",
            "message": f"Configured · sending from {from_addr} · alerts to {owner_email}",
            "configured": True,
        }
    elif api_key and from_addr:
        results["email"] = {
            "status": "warning",
            "message": "AGENTMAIL_API_KEY and AGENTMAIL_FROM are set, but OWNER_EMAIL is missing",
            "fix": "Set OWNER_EMAIL in .env to receive flow failure alerts.",
            "configured": True,
        }
    else:
        missing = [v for v, k in [("AGENTMAIL_API_KEY", api_key), ("AGENTMAIL_FROM", from_addr)] if not k]
        results["email"] = {
            "status": "warning",
            "message": f"Email not configured ({', '.join(missing)} not set) — alerts and password reset are disabled",
            "fix": "Sign up at agentmail.to, create an inbox, then set AGENTMAIL_API_KEY and AGENTMAIL_FROM in .env.",
            "configured": False,
        }

    # ── Credential encryption ─────────────────────────────────────────────────
    secret_key = os.environ.get("SECRET_KEY", "").strip()
    if not secret_key:
        results["encryption"] = {
            "status": "warning",
            "message": "SECRET_KEY not set — credentials stored with weak fallback key",
            "fix": (
                "Generate a key: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
                " then set SECRET_KEY in .env and restart."
            ),
            "configured": False,
        }
    else:
        results["encryption"] = {
            "status": "ok",
            "message": "Credentials encrypted at rest (Fernet / AES-128-CBC)",
            "configured": True,
        }

    # ── OAuth providers ───────────────────────────────────────────────────────
    oauth_configured = {
        "github":  bool(os.environ.get("GITHUB_CLIENT_ID", "").strip()),
        "google":  bool(os.environ.get("GOOGLE_CLIENT_ID", "").strip()),
        "notion":  bool(os.environ.get("NOTION_CLIENT_ID", "").strip()),
    }
    any_oauth = any(oauth_configured.values())
    results["oauth"] = {
        "status": "ok" if any_oauth else "warning",
        "message": (
            "Providers: " + ", ".join(k for k, v in oauth_configured.items() if v)
            if any_oauth else "No OAuth providers configured"
        ),
        "fix": "Set GITHUB/GOOGLE/NOTION _CLIENT_ID and _CLIENT_SECRET in .env to enable one-click credential connect." if not any_oauth else None,
        "providers": oauth_configured,
    }

    # ── Security posture ──────────────────────────────────────────────────────
    app_url = os.environ.get("APP_URL", "http://localhost")
    api_key_raw = os.environ.get("API_KEY", "")
    security_issues = []
    if not app_url.startswith("https://"):
        security_issues.append("APP_URL is not https — session cookies will not be Secure-flagged")
    if api_key_raw in ("dev_api_key", "change-me-before-deployment", ""):
        security_issues.append("API_KEY is set to an unsafe default or is blank")
    if security_issues:
        results["security"] = {
            "status": "warning",
            "message": "; ".join(security_issues),
            "fix": "Set APP_URL=https://yourdomain.com and change API_KEY to a random secret in .env.",
            "issues": security_issues,
        }
    else:
        results["security"] = {
            "status": "ok",
            "message": "HTTPS enabled · API key is non-default",
        }

    # ── Platform info ─────────────────────────────────────────────────────────
    results["system"] = {
        "status": "ok",
        "message": f"Python {sys.version.split()[0]} · {platform.system()} · PID {os.getpid()}",
        "app_version":  __version__,
        "python":       sys.version.split()[0],
        "platform":     platform.system() + " " + platform.release(),
        "hostname":     socket.gethostname(),
        "pid":          os.getpid(),
        "app_timezone": os.environ.get("APP_TIMEZONE", "UTC"),
        "app_url":      app_url,
        "allow_signup": os.environ.get("ALLOW_SIGNUP", "false").lower() == "true",
    }

    return results


# ── Version check ─────────────────────────────────────────────────────────────
_GITHUB_REPO    = "TurnrIO/HiveRunr"
_VERSION_CACHE_KEY = "version_check_cache"   # stored as JSON in app_settings
_VERSION_CACHE_TTL = 86400                   # 24 hours in seconds

@router.get("/api/version")
def api_version(request: Request):
    """Return current version + latest GitHub release (cached 24 h)."""
    import time
    from app.core.db import get_setting, set_setting
    _check_admin(request)

    # ── Try to return a cached latest-version result ───────────────────────
    cached_raw = get_setting(_VERSION_CACHE_KEY, None)
    cached = None
    if cached_raw:
        try:
            cached = json.loads(cached_raw)
            age = time.time() - cached.get("checked_at", 0)
            if age < _VERSION_CACHE_TTL:
                # Cache is fresh — return immediately
                cached["current"] = __version__
                cached["update_available"] = _version_gt(cached.get("latest", __version__), __version__)
                return cached
        except JSONDecodeError:
            cached = None

    # ── Fetch latest release from GitHub ──────────────────────────────────
    latest = __version__
    release_url = f"https://github.com/{_GITHUB_REPO}/releases/latest"
    error = None
    try:
        resp = httpx.get(
            f"https://api.github.com/repos/{_GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github+json", "User-Agent": f"HiveRunr/{__version__}"},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            tag = data.get("tag_name", "").lstrip("v")
            if tag:
                latest = tag
                release_url = data.get("html_url", release_url)
    except (httpx.HTTPError, OSError, JSONDecodeError) as exc:
        error = str(exc)

    payload = {
        "current":          __version__,
        "latest":           latest,
        "update_available": _version_gt(latest, __version__),
        "release_url":      release_url,
        "checked_at":       int(time.time()),
    }
    if error:
        payload["check_error"] = error

    # Cache result (even on error, to avoid hammering GH on every page load)
    try:
        set_setting(_VERSION_CACHE_KEY, json.dumps({k: v for k, v in payload.items() if k != "current"}))
    except (ValueError, AttributeError, TypeError):
        pass

    return payload


def _version_gt(a: str, b: str) -> bool:
    """Return True if semver string a is strictly greater than b."""
    try:
        def _parts(v):
            return tuple(int(x) for x in v.lstrip("v").split(".")[:3])
        return _parts(a) > _parts(b)
    except (ValueError, AttributeError):
        return False


# ── Metrics ───────────────────────────────────────────────────────────────────
@router.get("/api/metrics")
def api_metrics(request: Request):
    from app.core.db import get_run_metrics
    from app.deps import _resolve_workspace
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    return get_run_metrics(workspace_id=workspace_id)


@router.get("/api/analytics/flows")
def api_analytics_flows(request: Request, days: int = 30):
    """Per-flow performance stats (avg/P95/P99 duration, error rate)."""
    from app.core.db import get_flow_analytics
    from app.deps import _resolve_workspace
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    days = max(1, min(days, 365))
    return get_flow_analytics(days, workspace_id=workspace_id)


@router.get("/api/analytics/daily")
def api_analytics_daily(request: Request, days: int = 30):
    """Daily run volume + average duration."""
    from app.core.db import get_daily_analytics
    from app.deps import _resolve_workspace
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    days = max(1, min(days, 365))
    return get_daily_analytics(days, workspace_id=workspace_id)


# ── Run logs ──────────────────────────────────────────────────────────────────
RUNLOGS_DIR = Path("/app/runlogs")


@router.get("/api/runlogs")
def api_runlogs(request: Request):
    _check_admin(request)
    if not RUNLOGS_DIR.exists():
        return []
    return sorted([f.name for f in RUNLOGS_DIR.glob("*.log")], reverse=True)[:50]


@router.get("/api/runlogs/{filename}")
def api_runlog_file(filename: str, request: Request):
    _check_admin(request)
    p = (RUNLOGS_DIR / filename).resolve()
    try:
        p.relative_to(RUNLOGS_DIR.resolve())
    except ValueError:
        raise HTTPException(404)
    if not p.exists() or not p.name.endswith(".log"):
        raise HTTPException(404)
    return {"content": p.read_text(errors="replace")[-8000:]}


# ── Admin reset + maintenance ─────────────────────────────────────────────────
@router.post("/api/admin/reset")
def api_reset(request: Request):
    user = _check_admin(request)
    from app.core.db import get_conn
    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        try:
            for t in ["runs", "schedules", "graph_versions", "graph_workflows", "workflows"]:
                cur.execute(f"DELETE FROM {t}")
            conn.commit()
        except (AttributeError, RuntimeError, OSError, TypeError):
            conn.rollback()
            raise
    log_audit(user["username"], "admin.reset", None, None, {"tables": "runs,schedules,graph_versions,graph_workflows,workflows"},
              request.client.host if request.client else None)
    return {"reset": True}


@router.post("/api/maintenance/reset_sequences")
def api_reset_sequences(request: Request):
    """Reset all PostgreSQL SERIAL sequences back to 1."""
    user = _check_admin(request)
    from app.core.db import get_conn
    tables = [
        "runs", "workflows", "schedules", "graph_workflows",
        "credentials", "graph_versions", "users", "sessions", "api_tokens",
    ]
    with get_conn() as conn:
        cur = conn.cursor()
        for t in tables:
            cur.execute("SELECT setval(pg_get_serial_sequence(%s, 'id'), 1, false)", (t,))
    log_audit(user["username"], "admin.reset_sequences", None, None, None,
              request.client.host if request.client else None)
    return {"reset": True, "tables": tables}


# ── Node registry ─────────────────────────────────────────────────────────────
@router.get("/api/nodes")
def api_list_nodes(request: Request):
    _check_admin(request)
    from app.nodes import list_node_types
    return {"node_types": list_node_types()}


@router.post("/api/admin/reload_nodes")
def api_reload_nodes(request: Request):
    """Hot-reload custom nodes from app/nodes/custom/ without restarting."""
    _check_admin(request)
    from app.nodes import reload_custom, list_node_types
    reload_custom()
    return {"reloaded": True, "node_types": list_node_types()}


# ── Scripts ───────────────────────────────────────────────────────────────────
def _safe_script_name(name: str) -> str:
    if not _re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
        raise HTTPException(400, "Script name must start with a letter and contain only letters, digits, underscores")
    if name.startswith('_'):
        raise HTTPException(400, "Script names starting with _ are reserved")
    return name


@router.get("/api/scripts")
def api_list_scripts(request: Request):
    _check_admin(request)
    scripts = []
    for p in sorted(SCRIPTS_DIR.glob("*.py")):
        if p.stem.startswith("_"):
            continue
        scripts.append({"name": p.stem, "size": p.stat().st_size, "modified": p.stat().st_mtime})
    return scripts


@router.get("/api/scripts/{name}")
def api_get_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists():
        raise HTTPException(404, "Script not found")
    return {"name": name, "content": p.read_text(errors="replace")}


@router.post("/api/scripts")
async def api_create_script(request: Request):
    _check_admin(request)
    content_length = request.headers.get("content-length")
    try:
        if content_length and int(content_length) > MAX_SCRIPT_SIZE_BYTES:
            raise HTTPException(413, f"Script content too large — maximum {MAX_SCRIPT_SIZE_BYTES // 1024} KB")
    except ValueError:
        raise HTTPException(400, "Invalid Content-Length header")
    body = await request.json()
    # Reject suspiciously large bodies even if Content-Length was not set
    if len(json.dumps(body)) > MAX_SCRIPT_SIZE_BYTES:
        raise HTTPException(413, f"Script content too large — maximum {MAX_SCRIPT_SIZE_BYTES // 1024} KB")
    name = _safe_script_name(body.get("name", ""))
    content = body.get("content", "# New script\n")
    p = SCRIPTS_DIR / f"{name}.py"
    if p.exists():
        raise HTTPException(409, "Script already exists")
    p.write_text(content)
    return {"name": name, "created": True}


@router.put("/api/scripts/{name}")
async def api_update_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    content_length = request.headers.get("content-length")
    try:
        if content_length and int(content_length) > MAX_SCRIPT_SIZE_BYTES:
            raise HTTPException(413, f"Script content too large — maximum {MAX_SCRIPT_SIZE_BYTES // 1024} KB")
    except ValueError:
        raise HTTPException(400, "Invalid Content-Length header")
    body = await request.json()
    # Reject suspiciously large bodies even if Content-Length was not set
    if len(json.dumps(body)) > MAX_SCRIPT_SIZE_BYTES:
        raise HTTPException(413, f"Script content too large — maximum {MAX_SCRIPT_SIZE_BYTES // 1024} KB")
    content = body.get("content", "")
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists():
        raise HTTPException(404, "Script not found")
    p.write_text(content)
    return {"name": name, "saved": True}


@router.delete("/api/scripts/{name}")
def api_delete_script(name: str, request: Request):
    _check_admin(request)
    _safe_script_name(name)
    p = SCRIPTS_DIR / f"{name}.py"
    if not p.exists():
        raise HTTPException(404, "Script not found")
    try:
        p.unlink()
    except (OSError, PermissionError, FileNotFoundError) as exc:
        log.error("Could not delete script %s: %s", name, exc)
        raise HTTPException(500, f"Could not delete script: {exc}")
    return {"name": name, "deleted": True}


@router.post("/api/scripts/{name}/run")
async def api_run_script(name: str, request: Request):
    user = _check_admin(request)
    _safe_script_name(name)
    try:
        payload = await request.json()
    except (JSONDecodeError, UnicodeDecodeError):
        payload = {}
    from app.core.db import get_conn
    from app.deps import _resolve_workspace
    from app.worker import enqueue_script
    task_id      = str(uuid.uuid4())
    workspace_id = _resolve_workspace(request, user)
    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO runs(task_id, workflow, status, workspace_id) VALUES(%s,%s,'queued',%s)",
            (task_id, name, workspace_id)
        )
    enqueue_script.apply_async(args=[name, payload], task_id=task_id)
    return {"task_id": task_id, "workflow": name}


# ── Audit log ─────────────────────────────────────────────────────────────────
# NOTE: Template endpoints (GET /api/templates, POST /api/templates/{slug}/use,
# GET /api/templates/{slug}) are owned by app/routers/templates.py.
# Duplicates were removed from here to prevent silent shadowing.
@router.get("/api/audit-log")
def api_get_audit_log(
    request: Request,
    limit:  int           = Query(100, ge=1, le=500),
    offset: int           = Query(0,   ge=0),
    actor:  Optional[str] = Query(None),
    action: Optional[str] = Query(None),
):
    """Return audit log entries (owner only).  Newest first."""
    _require_owner(request)
    rows = get_audit_log(limit=limit, offset=offset, actor=actor, action=action)
    # Serialize datetime objects to ISO strings for JSON
    for r in rows:
        if r.get("created_at"):
            r["created_at"] = r["created_at"].isoformat()
    return rows
