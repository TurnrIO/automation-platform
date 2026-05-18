"""HiveRunr — FastAPI application entry point.

All API routes live in app/routers/. This file wires together the app,
static files, page routes, and startup lifecycle only.
"""
import os
import logging
import psycopg2
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.core.secrets import load_secrets
load_secrets()  # populate os.environ from provider BEFORE any other imports use env vars

from app.core.db import init_db, list_workflows, upsert_workflow
from app.worker import enqueue_workflow
from app.deps import _auth_redirect
from app.seeds import seed_example_graphs
from app.observability import configure_logging, PrometheusMiddleware
from app.telemetry import setup_tracing

# ── Structured JSON logging (must run before any other log calls) ─────────────
configure_logging()

# ── Distributed tracing (no-op when OTEL_EXPORTER_OTLP_ENDPOINT is unset) ────
setup_tracing()

# ── Routers ───────────────────────────────────────────────────────────────────
from app.routers.auth        import router as auth_router
from app.routers.graphs      import router as graphs_router
from app.routers.runs        import router as runs_router
from app.routers.schedules   import router as schedules_router
from app.routers.credentials import router as credentials_router
from app.routers.webhooks    import router as webhooks_router
from app.routers.admin       import router as admin_router
from app.routers.workspaces  import router as workspaces_router
from app.routers.oauth       import router as oauth_router
from app.routers.templates   import router as templates_router
from app.routers.approvals   import router as approvals_router

log          = logging.getLogger(__name__)
STATIC_DIR   = Path(__file__).parent / "static"
# DIST_DIR: prefer /app/frontend_dist/ (Docker image, outside bind mount) so
# the ./app bind mount in docker-compose doesn't clobber the built assets.
# Fall back to app/static/dist/ for local "npm run dev" workflows.
_docker_dist = Path("/app/frontend_dist")
DIST_DIR     = _docker_dist if _docker_dist.is_dir() else STATIC_DIR / "dist"
WORKFLOWS    = ["example"]
# Fail fast: an unset API_KEY is a deployment error, not a warning.
# The app will not start until a secure, non-default key is configured.
API_KEY = os.environ.get("API_KEY", "")
if not API_KEY or API_KEY in {"dev_api_key", "change-me-before-deployment"}:
    raise RuntimeError(
        "API_KEY environment variable is not set or is set to an insecure default. "
        "Set API_KEY to a random secret in .env before deploying this instance. "
        "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(32))\""
    )

# ── F-series migration tracker ────────────────────────────────────────────────
# Pages listed here are served from the Vite dist/ build instead of app/static/.
# Add the filename when the corresponding F-sprint is complete and confirmed.
#   F2  → "login.html", "signup.html", "reset.html", "invite.html"
#   F6  → "admin.html"
#   F9  → "canvas.html"
_MIGRATED_PAGES: set[str] = {
    # F2 — auth pages migrated to Vite + React
    "login.html",
    "signup.html",
    "reset.html",
    "invite.html",
    # F6 — admin SPA migrated to Vite + React
    "admin.html",
    # F9 — canvas editor migrated to Vite + React
    "canvas.html",
}


def _serve_page(filename: str):
    """Serve from Vite dist/ if migrated, else fall back to legacy app/static/."""
    if filename in _MIGRATED_PAGES:
        dist_file = DIST_DIR / filename
        if dist_file.exists():
            return FileResponse(str(dist_file), media_type="text/html")
        # dist not found — give an actionable error instead of a cryptic 500
        log.error(
            "Frontend dist missing: %s not found. DIST_DIR=%s. "
            "Run 'docker compose up -d --build' to rebuild the image.",
            filename, DIST_DIR,
        )
        from fastapi.responses import HTMLResponse as _HTML
        return _HTML(
            "<html><body style='font:16px sans-serif;padding:40px;"
            "background:#0f1117;color:#e2e8f0'>"
            f"<h2>⚠ Frontend assets not built</h2>"
            f"<p><code>{filename}</code> not found in <code>{DIST_DIR}</code>.</p>"
            "<p>Fix: <code>docker compose up -d --build</code></p>"
            "</body></html>",
            status_code=503,
        )
    legacy = STATIC_DIR / filename
    if not legacy.exists():
        from fastapi.responses import HTMLResponse as _HTML
        log.error("Legacy page missing: %s", legacy)
        return _HTML(f"<html><body>Page {filename} not found.</body></html>", status_code=404)
    return FileResponse(str(legacy), media_type="text/html")

from app._version import __version__


def _startup():
    """Synchronous startup logic — runs inside the lifespan manager."""
    try:
        _validate_config()
    except RuntimeError as exc:
        log.error("startup: FATAL — %s", exc)
        import sys; sys.exit(1)
    init_db()
    seed_example_graphs()
    for name in WORKFLOWS:
        try:
            upsert_workflow(name)
        except (AttributeError, RuntimeError, OSError) as exc:
            log.warning("startup: could not register workflow %r — %s", name, exc)


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Lifespan context manager — replaces @on_event startup/shutdown."""
    _startup()
    yield


app = FastAPI(
    title="HiveRunr",
    version=__version__,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=_lifespan,
)
# ── Global exception handler ──────────────────────────────────────────────────
# Catches any unhandled exception that reaches the top of the stack, logs the
# full traceback, then returns a structured JSON 500 response.
# This ensures every server error leaves a trace — previously psycopg2 errors
# and import failures in handler bodies could produce 500s with zero log output.
import traceback as _traceback
from fastapi import Request as _Request
from fastapi.responses import JSONResponse as _JSONResponse
from starlette.exceptions import HTTPException as _StarletteHTTP

@app.exception_handler(Exception)
async def _unhandled_exception_handler(_req: _Request, exc: Exception) -> _JSONResponse:
    # Let FastAPI's own HTTPException handler deal with intentional HTTP errors
    if isinstance(exc, _StarletteHTTP):
        raise exc
    log.error(
        "Unhandled exception %s %s: %s\n%s",
        _req.method, _req.url.path, exc,
        _traceback.format_exc(),
    )
    return _JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {type(exc).__name__}: {exc}"},
    )

# ── Middleware ────────────────────────────────────────────────────────────────
app.add_middleware(PrometheusMiddleware)

# ── Rate limiting (slowapi + Redis) ───────────────────────────────────────────
# Note: we import and configure this AFTER app creation so `app` is in scope.
# The limiter is shared as app.state.limiter so route decorators can reference it.
from app.routers._rate_limit import limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Defence-in-depth: restrict cross-origin access. The React SPA and API are
# same-origin in normal deployment, but explicit allowlist prevents bypass if
# Caddy misconfiguration occurs.
from fastapi.middleware.cors import CORSMiddleware
_app_url = os.environ.get("APP_URL", "http://localhost")
_parsed = __import__("urllib.parse").urlparse(_app_url)
_allowed_origins_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "").strip()
if not _allowed_origins_raw:
    _allowed_origins_raw = f"{_parsed.scheme}://{_parsed.netloc}"
_origins = [o.strip() for o in _allowed_origins_raw.split(",") if o.strip()]
if _origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "x-api-token", "X-Workspace-Id"],
        expose_headers=["X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Policy"],
        max_age=86400,
    )

# ── Include routers ───────────────────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(graphs_router)
app.include_router(runs_router)
app.include_router(schedules_router)
app.include_router(credentials_router)
app.include_router(webhooks_router)
app.include_router(admin_router)
app.include_router(workspaces_router)
app.include_router(oauth_router)
app.include_router(templates_router)
app.include_router(approvals_router)

# ── Duplicate route guard ─────────────────────────────────────────────────────
# Detects shadowed routes at startup so problems are caught immediately
# rather than silently serving the wrong handler in production.
_seen_routes: dict = {}
_dup_warnings: list = []
for _r in app.routes:
    if not hasattr(_r, "methods"):
        continue
    for _m in _r.methods:
        _key = f"{_m} {getattr(_r, 'path', '?')}"
        _owner = getattr(getattr(_r, "endpoint", None), "__module__", "?")
        if _key in _seen_routes:
            _dup_warnings.append(f"{_key}  ({_seen_routes[_key]} shadowed by {_owner})")
        else:
            _seen_routes[_key] = _owner
if _dup_warnings:
    import logging as _lg
    for _w in _dup_warnings:
        _lg.getLogger(__name__).error("DUPLICATE ROUTE DETECTED: %s", _w)

# ── Static files ──────────────────────────────────────────────────────────────
# IMPORTANT: /static/dist must be mounted BEFORE /static so Starlette's
# first-match routing serves the Vite assets from DIST_DIR instead of the
# empty app/static/dist/ directory on the host.
if DIST_DIR.is_dir():
    app.mount("/static/dist", StaticFiles(directory=str(DIST_DIR)), name="static_dist")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return RedirectResponse("/static/favicon.svg", status_code=302)


# ── Startup validation ────────────────────────────────────────────────────────
def _validate_config() -> None:
    """Check critical env vars and dependencies at startup.

    Logs a clear actionable WARNING for every problem found.
    Raises RuntimeError for anything that will make the app non-functional
    (missing DB or Redis).  All other issues are warnings only — the app
    still starts so you can use the System page to diagnose them.
    """

    # ── Fatal: Database ───────────────────────────────────────────────────────
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Add it to .env, e.g.: DATABASE_URL=postgresql://user:pass@db:5432/hiverunr"
        )
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            conn.cursor().execute("SELECT 1")
        log.info("startup: database connection OK")
    except (OSError, RuntimeError, AttributeError) as exc:
        raise RuntimeError(
            f"Cannot connect to the database: {exc}. "
            "Check DATABASE_URL in .env and ensure the postgres container is running."
        ) from exc

    # ── Fatal: Redis ──────────────────────────────────────────────────────────
    redis_url = os.environ.get("REDIS_URL", "")
    if not redis_url:
        raise RuntimeError(
            "REDIS_URL is not set. "
            "Add it to .env, e.g.: REDIS_URL=redis://redis:6379/0"
        )
    try:
        import redis as _redis
        _redis.from_url(redis_url, socket_connect_timeout=3).ping()
        log.info("startup: Redis connection OK")
    except (OSError, RuntimeError, AttributeError) as exc:
        raise RuntimeError(
            f"Cannot connect to Redis: {exc}. "
            "Check REDIS_URL in .env and ensure the redis container is running."
        ) from exc

    # ── Warning: SECRET_KEY ───────────────────────────────────────────────────
    if not os.environ.get("SECRET_KEY", "").strip():
        log.warning(
            "startup: SECRET_KEY is not set — credentials are stored with a weak "
            "fallback key. Generate one with: "
            "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
            " then set SECRET_KEY in .env."
        )

    # ── Warning: API_KEY ──────────────────────────────────────────────────────
    # API_KEY is now enforced at module level — no fallback default.
    # (The check above guarantees it is set and non-unsafe at startup.)

    # ── Warning: APP_URL ──────────────────────────────────────────────────────
    app_url = os.environ.get("APP_URL", "http://localhost")
    if not app_url.startswith("https://"):
        log.warning(
            "startup: APP_URL is '%s' (not https). "
            "Session cookies will not be Secure-flagged. "
            "Set APP_URL=https://yourdomain.com in .env for production use.", app_url
        )

    # ── Warning: Email ────────────────────────────────────────────────────────
    if not os.environ.get("AGENTMAIL_API_KEY", "").strip():
        log.warning(
            "startup: AGENTMAIL_API_KEY is not set — email alerts and "
            "password reset are disabled. Sign up at agentmail.to to enable them."
        )

    # ── Warning: OWNER_EMAIL ──────────────────────────────────────────────────
    if not os.environ.get("OWNER_EMAIL", "").strip():
        log.warning(
            "startup: OWNER_EMAIL is not set — flow failure alerts will not be delivered. "
            "Set OWNER_EMAIL in .env."
        )


# ── Startup (deprecated) ──────────────────────────────────────────────────────
# Replaced by _lifespan below. Kept as a no-op reference.
def startup():
    pass










# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    checks = {}
    overall = "ok"

    # ── Database ─────────────────────────────────────────────────────────────
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        checks["database"] = {"status": "ok"}
    except (OSError, RuntimeError, AttributeError, Exception) as exc:
        checks["database"] = {
            "status": "error",
            "message": str(exc),
            "fix": "Check DATABASE_URL in .env and ensure the postgres container is running.",
        }
        overall = "degraded"

    # ── Redis ─────────────────────────────────────────────────────────────────
    try:
        import redis as _redis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = _redis.from_url(redis_url, socket_connect_timeout=3, socket_timeout=3)
        r.ping()
        display_url = redis_url.split("@")[-1]  # strip credentials if present
        checks["redis"] = {"status": "ok", "url": display_url}
    except (OSError, RuntimeError, AttributeError, Exception) as exc:
        checks["redis"] = {
            "status": "error",
            "message": str(exc),
            "fix": "Check REDIS_URL in .env and ensure the redis container is running.",
        }
        overall = "degraded"

    # ── Frontend dist ─────────────────────────────────────────────────────────
    dist_available = _docker_dist.is_dir() and any(_docker_dist.iterdir()) if _docker_dist.exists() else False
    checks["frontend_dist"] = {
        "status": "ok" if dist_available else "error",
        "path": str(DIST_DIR),
        "fix": "run 'docker compose up -d --build' if unavailable" if not dist_available else "ok",
    }
    if not dist_available:
        overall = "degraded"

    return {
        "status": overall,
        "version": __version__,
        "checks": checks,
    }


# ── Healthz (k8s readiness) ─────────────────────────────────────────────────
@app.get("/healthz")
def healthz():
    """Kubernetes readiness probe — returns 200 if DB + Redis are reachable."""
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except (psycopg2.Error, OSError):
        return Response("database unreachable", status_code=503)
    try:
        import redis as _redis
        from redis.exceptions import ConnectionError as _RedisConnError, TimeoutError as _RedisTimeoutError
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = _redis.from_url(redis_url, socket_connect_timeout=3, socket_timeout=3)
        r.ping()
    except (_RedisConnError, _RedisTimeoutError, OSError):
        return Response("redis unreachable", status_code=503)
    return Response("ok", media_type="text/plain")


# ── Page routes ───────────────────────────────────────────────────────────────
@app.get("/login")
def login_page(request: Request):
    from app.core.db import users_exist
    if not users_exist():
        return RedirectResponse("/setup", status_code=302)
    from app.auth import get_current_user
    if get_current_user(request):
        return RedirectResponse("/", status_code=302)
    return _serve_page("login.html")


@app.get("/setup")
def setup_page(request: Request):
    from app.core.db import users_exist
    if users_exist():
        return RedirectResponse("/", status_code=302)
    return FileResponse(str(STATIC_DIR / "setup.html"), media_type="text/html")


@app.get("/")
def root(request: Request):
    redir = _auth_redirect(request)
    if redir:
        return redir
    return _serve_page("admin.html")


@app.get("/admin")
def admin_redirect(request: Request):
    """Redirect /admin → / so the React SPA routes work correctly."""
    redir = _auth_redirect(request)
    if redir:
        return redir
    return RedirectResponse("/", status_code=302)


@app.get("/admin/{rest:path}")
def admin_path_redirect(request: Request, rest: str = ""):
    """Redirect /admin/<path> → /<path> (e.g. /admin/graphs → /graphs)."""
    redir = _auth_redirect(request)
    if redir:
        return redir
    return RedirectResponse(f"/{rest}", status_code=302)


# Admin SPA sub-routes — direct navigation / page refresh support.
# React Router handles these client-side; FastAPI must serve admin.html for each.
_ADMIN_SPA_PATHS = [
    "graphs", "templates", "metrics", "scripts", "credentials",
    "schedules", "logs", "users", "audit", "settings", "workspaces", "system",
]


def _make_admin_spa_handler():
    def handler(request: Request):
        redir = _auth_redirect(request)
        if redir:
            return redir
        return _serve_page("admin.html")
    return handler


for _path in _ADMIN_SPA_PATHS:
    app.add_api_route(f"/{_path}", _make_admin_spa_handler(), methods=["GET"], include_in_schema=False)


@app.get("/reset-password")
def reset_password_page():
    return _serve_page("reset.html")


@app.get("/invite/accept")
def invite_accept_page():
    return _serve_page("invite.html")


@app.get("/canvas")
@app.get("/admin/canvas")
def canvas_page(request: Request):
    redir = _auth_redirect(request)
    if redir:
        return redir
    return _serve_page("canvas.html")


# ── Auth-gated API docs ───────────────────────────────────────────────────────
@app.get("/openapi.json", include_in_schema=False)
def openapi_schema(request: Request):
    redir = _auth_redirect(request)
    if redir:
        return redir
    return JSONResponse(app.openapi())


@app.get("/docs", include_in_schema=False)
def docs_page(request: Request):
    redir = _auth_redirect(request)
    if redir:
        return redir
    from fastapi.openapi.docs import get_swagger_ui_html
    return get_swagger_ui_html(openapi_url="/openapi.json", title="HiveRunr API")


@app.get("/redoc", include_in_schema=False)
def redoc_page(request: Request):
    redir = _auth_redirect(request)
    if redir:
        return redir
    from fastapi.openapi.docs import get_redoc_html
    return get_redoc_html(openapi_url="/openapi.json", title="HiveRunr API")


# ── Legacy workflow trigger ───────────────────────────────────────────────────
def _check_api_key(key: str):
    if API_KEY and key != API_KEY:
        raise HTTPException(401, "Invalid API key")


class RunRequest(BaseModel):
    payload: dict = {}


@app.post("/run/{workflow}")
def run_workflow(workflow: str, req: RunRequest, x_api_key: str = Header(default="")):
    _check_api_key(x_api_key)
    if workflow not in WORKFLOWS:
        raise HTTPException(404, "Unknown workflow")
    task = enqueue_workflow.delay(workflow, req.payload)
    return {"queued": True, "task_id": task.id, "workflow": workflow}


# ── Workflows (legacy script runner) ─────────────────────────────────────────
@app.get("/api/workflows")
def api_workflows(request: Request):
    from app.deps import _check_admin
    _check_admin(request)
    return list_workflows()


@app.post("/api/workflows/{name}/toggle")
def api_toggle_workflow(name: str, request: Request):
    from app.deps import _require_manage_scope
    from app.core.db import toggle_workflow
    _require_manage_scope(request)
    return toggle_workflow(name) or {"name": name}


@app.post("/api/workflows/{name}/run")
async def api_run_workflow(name: str, request: Request):
    import uuid as _uuid
    from app.deps import _require_run_scope, _resolve_workspace
    from app.core.db import get_conn, list_workflows
    from app.worker import enqueue_script
    user = _require_run_scope(request)
    # Check the workflow exists and is enabled
    workflows = {w["name"]: w for w in list_workflows()}
    if name not in workflows:
        raise HTTPException(404, f"Workflow '{name}' not found")
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            payload = {}
    except (ValueError, TypeError):
        payload = {}
    task_id      = str(_uuid.uuid4())
    workspace_id = _resolve_workspace(request, user)
    with get_conn() as conn:
        conn.cursor().execute(
            "INSERT INTO runs(task_id, workflow, status, workspace_id) VALUES(%s, %s, 'queued', %s)",
            (task_id, name, workspace_id)
        )
    enqueue_script.apply_async(args=[name, payload], task_id=task_id)
    return {"queued": True, "task_id": task_id, "workflow": name}

