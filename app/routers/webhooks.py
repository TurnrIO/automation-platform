"""Webhook trigger router + rate limiting."""
import hashlib
import hmac
import json
from json import JSONDecodeError
import logging
import psycopg2
import redis
import os

GRAPH_IMPORT_MAX_BYTES = 10 * 1024 * 1024  # 10 MB — matches graphs.py limit

log = logging.getLogger(__name__)

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.core.db import get_graph_by_token, get_ratelimit_policy
from app.worker import enqueue_graph
from app.core.executor import run_graph

router = APIRouter()


def _check_webhook_rate(token: str) -> tuple[bool, int, int]:
    """Returns (allowed, limit, window). Reads config live from app_settings."""
    policy = get_ratelimit_policy()
    limit  = policy["limit"]
    window = policy["window"]
    if limit <= 0:
        return True, limit, window
    try:
        import redis as _redis
        r = _redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"))
        key = f"wh_rate:{token}"
        pipe = r.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        count, _ = pipe.execute()
        return count <= limit, limit, window
    except (redis.exceptions.RedisError, OSError, TimeoutError):  # Redis connection/network failures → fail closed
        log.warning(f"Redis unavailable for webhook rate limit check: {token} — fail closed, returning denied")
        return False, limit, window


def _get_webhook_trigger_config(g: dict) -> dict:
    """Extract the config of the first trigger.webhook node from graph_data."""
    try:
        graph_data = g.get("graph_data") or {}
        if isinstance(graph_data, str):
            graph_data = json.loads(graph_data)
        nodes = graph_data.get("nodes", [])
        for node in nodes:
            if node.get("type") == "trigger.webhook" or node.get("data", {}).get("type") == "trigger.webhook":
                return node.get("data", {}).get("config", {})
    except JSONDecodeError:
        log.warning("webhook trigger config parse failed: graph_data is not valid JSON")
    except (AttributeError, KeyError, TypeError) as exc:
        log.warning("webhook trigger config parse failed accessing graph_data: %s", exc)
    return {}


def _verify_hmac(secret: str, body: bytes, signature_header: str) -> bool:
    """Verify X-Hub-Signature-256 header (same convention as GitHub webhooks)."""
    if not signature_header:
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature_header)


@router.post("/webhook/{token}")
async def webhook_trigger(token: str, request: Request):
    g = get_graph_by_token(token)
    if not g:
        raise HTTPException(404, "Unknown webhook token")
    if not g.get("enabled"):
        raise HTTPException(403, "Graph is disabled")

    # ── Workspace ownership guard ────────────────────────────────────────────
    # External callers (GitHub, Zapier, etc.) have no workspace context — they
    # only prove they hold the webhook token.  Internal API clients that DO have
    # a workspace identity (via X-Workspace-Id header) must match the graph's
    # workspace so a token for Graph-A in Workspace-1 cannot be used to trigger
    # a graph whose workspace is a different one.
    caller_ws = request.headers.get("X-Workspace-Id", "").strip()
    if caller_ws:
        try:
            caller_ws_int = int(caller_ws)
            graph_ws = g.get("workspace_id")
            if graph_ws is not None and caller_ws_int != graph_ws:
                raise HTTPException(
                    403,
                    f"Webhook token is not valid for workspace {caller_ws_int}",
                )
        except ValueError:
            raise HTTPException(400, "X-Workspace-Id must be an integer")

    allowed, limit, window = _check_webhook_rate(token)
    if not allowed:
        raise HTTPException(429, f"Rate limit exceeded — max {limit} calls per {window}s")

    cfg = _get_webhook_trigger_config(g)

    # ── CORS / allowed origins ─────────────────────────────────────────────
    allowed_origins_raw = cfg.get("allowed_origins", "").strip()
    origin = request.headers.get("origin", "")
    if allowed_origins_raw and origin:
        allowed_list = [o.strip() for o in allowed_origins_raw.splitlines() if o.strip()]
        if allowed_list and origin not in allowed_list and "*" not in allowed_list:
            raise HTTPException(403, f"Origin '{origin}' is not allowed")

    # ── Read raw body (needed for HMAC verification before parsing JSON) ───
    body = await request.body()

    # ── Size guard ─────────────────────────────────────────────────────────
    if len(body) > GRAPH_IMPORT_MAX_BYTES:
        raise HTTPException(413, f"Payload too large — maximum {GRAPH_IMPORT_MAX_BYTES // (1024*1024)} MB")

    # ── HMAC-SHA256 signature verification ────────────────────────────────
    secret = cfg.get("secret", "").strip()
    if secret:
        sig_header = request.headers.get("x-hub-signature-256", "")
        if not sig_header:
            raise HTTPException(401, "Missing X-Hub-Signature-256 header (HMAC secret configured)")
        if not _verify_hmac(secret, body, sig_header):
            raise HTTPException(401, "Invalid webhook signature")

    # ── Parse payload ──────────────────────────────────────────────────────
    try:
        payload = json.loads(body) if body else {}
    except JSONDecodeError:
        payload = {}

    workspace_id = g.get("workspace_id")

    task_id = None
    try:
        task = enqueue_graph.apply_async(
            args=[g["id"], payload],
            priority=g.get("priority", 5),
        )
        task_id = task.id
    except (OSError, RuntimeError, AttributeError) as exc:
        log.warning("Celery unavailable (%s) — running webhook graph inline", exc)
        import uuid
        task_id = str(uuid.uuid4())
        try:
            from app.core.db import init_db as _init_db, update_run
            _init_db()
            update_run(task_id, "running")
            try:
                graph_data = json.loads(g.get('graph_json') or '{}')
            except JSONDecodeError:
                graph_data = {}
            result = run_graph(
                graph_data,
                payload,
                workspace_id=workspace_id,
            )
            update_run(task_id, "succeeded", result=result,
                       traces=result.get('traces', []))
        except (OSError, RuntimeError) as infra_err:
            log.exception("Inline webhook graph run failed")
            update_run(task_id, "failed", result={"error": str(infra_err)})
            raise HTTPException(500, f"Graph run failed: {infra_err}")

    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            conn.cursor().execute(
                "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id) VALUES(%s,%s,'queued',%s,%s)",
                (task_id, g["id"], json.dumps(payload), workspace_id)
            )
    except psycopg2.Error as exc:
        log.warning("Could not record webhook run: %s", exc)
    except (AttributeError, RuntimeError, OSError) as exc:
        log.warning("Unexpected error recording webhook run: %s", exc)

    # ── CORS response headers ──────────────────────────────────────────────
    headers = {}
    if allowed_origins_raw and origin:
        headers["Access-Control-Allow-Origin"] = origin
    elif not allowed_origins_raw:
        headers["Access-Control-Allow-Origin"] = "*"

    return JSONResponse(
        {"queued": True, "task_id": task_id, "graph": g["name"]},
        headers=headers,
    )


@router.options("/webhook/{token}")
async def webhook_preflight(token: str, request: Request):
    """Handle CORS preflight for webhook endpoints."""
    g = get_graph_by_token(token)
    cfg = _get_webhook_trigger_config(g) if g else {}
    allowed_origins_raw = cfg.get("allowed_origins", "").strip()
    origin = request.headers.get("origin", "*")

    allow_origin = "*"
    if allowed_origins_raw:
        allowed = [o.strip() for o in allowed_origins_raw.splitlines() if o.strip()]
        allow_origin = origin if origin in allowed or "*" in allowed else "null"

    return JSONResponse(
        {},
        headers={
            "Access-Control-Allow-Origin":  allow_origin,
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, X-Hub-Signature-256",
            "Access-Control-Max-Age":       "86400",
        },
    )
