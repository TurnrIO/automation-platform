"""Approvals router — public endpoints for approve/reject links in emails.

GET /api/approvals/{token}/approve   — record approval, show confirmation page
GET /api/approvals/{token}/reject    — record rejection, show confirmation page
GET /api/approvals/{token}/status    — JSON status check (authenticated)
GET /api/approvals                   — list recent approvals (authenticated admin)
"""
import os
import logging
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.core.db import get_conn
from app.deps import _check_admin

log    = logging.getLogger(__name__)
router = APIRouter()

# ── HTML helper ───────────────────────────────────────────────────────────────

def _page(icon: str, heading: str, body: str, colour: str) -> HTMLResponse:
    app_url = os.environ.get("APP_URL", "http://localhost").rstrip("/")
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>HiveRunr — {heading}</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{background:#0d0f1a;color:#e2e8f0;
         font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
         display:flex;align-items:center;justify-content:center;min-height:100vh;padding:24px}}
    .card{{background:#13151f;border:1px solid #1e2130;border-radius:16px;
           padding:40px 44px;max-width:440px;width:100%;text-align:center;
           box-shadow:0 8px 40px #0006}}
    .icon{{font-size:52px;margin-bottom:16px}}
    h1{{font-size:22px;font-weight:700;color:{colour};margin-bottom:10px}}
    p{{font-size:14px;color:#64748b;line-height:1.7;margin-bottom:6px}}
    .back{{display:inline-block;margin-top:28px;padding:10px 22px;
           background:linear-gradient(135deg,#7c3aed,#6d28d9);
           color:#fff;text-decoration:none;border-radius:8px;
           font-size:13px;font-weight:600}}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h1>{heading}</h1>
    {body}
    <a class="back" href="{app_url}">← Open HiveRunr</a>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


# ── Approve ───────────────────────────────────────────────────────────────────

@router.get("/api/approvals/{token}/approve", include_in_schema=False)
def do_approve(token: str, request: Request):
    return _decide(token, "approved", request)


# ── Reject ────────────────────────────────────────────────────────────────────

@router.get("/api/approvals/{token}/reject", include_in_schema=False)
def do_reject(token: str, request: Request):
    return _decide(token, "rejected", request)


# ── Core decision handler ─────────────────────────────────────────────────────

def _decide(token: str, decision: str, request: Request | None = None) -> HTMLResponse:
    """Write decision to Redis + DB, return a confirmation page."""
    from app.deps import _resolve_workspace
    from app.auth import get_current_user

    # Validate token
    row = _get_approval(token)
    if not row:
        return _page(
            "❓", "Link not found",
            "<p>This approval link is invalid or has expired.</p>",
            "#f87171",
        )

    # Enforce workspace isolation: if the caller has a workspace context,
    # verify the approval belongs to that workspace.
    if request is not None:
        actor = get_current_user(request)
        if actor:
            workspace_id = _resolve_workspace(request, actor)
            if workspace_id is not None and row.get("workspace_id") is not None:
                if row["workspace_id"] != workspace_id:
                    return _page(
                        "🚫", "Wrong workspace",
                        "<p>This approval does not belong to your workspace.</p>",
                        "#f87171",
                    )

    if row["status"] != "pending":
        status = row["status"]
        label  = {"approved": "already approved ✅", "rejected": "already rejected ❌",
                  "expired": "expired ⌛"}.get(status, status)
        return _page(
            "ℹ️", "Already decided",
            f"<p>This approval request was {label}.</p>",
            "#94a3b8",
        )

    # Write to Redis first (node is polling this)
    try:
        import redis as _redis
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = _redis.from_url(redis_url, socket_connect_timeout=5)
        r.setex(f"approval:{token}:decision", 86400 * 7, decision)
    except _redis.RedisError as exc:
        log.error("approvals: Redis error for token %s — %s", token[:8], exc)
        return _page(
            "⚠️", "Error",
            "<p>Could not record your decision — the workflow server may be unavailable. "
            "Please try again or contact your administrator.</p>",
            "#fbbf24",
        )

    # Update DB
    try:
        with get_conn() as conn:
            conn.cursor().execute(
                "UPDATE approvals SET status=%s, decided_at=NOW() WHERE token=%s AND status='pending'",
                (decision, token),
            )
    except (AttributeError, RuntimeError, OSError) as exc:
        log.error("approvals: DB update failed for %s — %s", token[:8], exc)
        return _page(
            "⚠", "Error",
            "<p>Could not record your decision — the database may be unavailable. "
            "Please try again or contact your administrator.</p>",
            "#fbbf24",
        )

    log.info("approvals: %s decision=%s graph=%s", token[:8], decision, row.get("graph_name", ""))

    if decision == "approved":
        return _page(
            "✅", "Approved",
            f"<p>Your approval has been recorded. The workflow "
            f"<strong>{row.get('graph_name', '')}</strong> will continue.</p>",
            "#4ade80",
        )
    else:
        return _page(
            "❌", "Rejected",
            f"<p>Your rejection has been recorded. The workflow "
            f"<strong>{row.get('graph_name', '')}</strong> has been stopped.</p>",
            "#f87171",
        )


# ── Status check (authenticated) ─────────────────────────────────────────────

@router.get("/api/approvals/{token}/status")
def approval_status(token: str, request: Request):
    from app.auth import get_current_user
    if not get_current_user(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    row = _get_approval(token)
    if not row:
        return JSONResponse({"error": "Not found"}, status_code=404)
    return {k: row[k] for k in ("token", "status", "approver_email", "graph_name",
                                 "subject", "created_at", "decided_at", "timeout_hours")
            if k in row}


# ── List all approvals (admin) ────────────────────────────────────────────────

@router.get("/api/approvals")
def list_approvals(request: Request, status: str = "", limit: int = 50):
    from app.auth import get_current_user
    from app.deps import _resolve_workspace
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    with get_conn() as conn:
        cur = conn.cursor()
        is_global_owner = user.get("role") == "owner"
        # Check if workspace_id column exists before using it
        col_exists = False
        try:
            cur.execute("SELECT 1 FROM information_schema.columns WHERE table_name='approvals' AND column_name='workspace_id'")
            col_exists = cur.fetchone() is not None
        except (AttributeError, TypeError, RuntimeError):
            pass
        # Scope: always scope to workspace if one is contextually available,
        # unless the user is the global owner (who can see all workspaces)
        if col_exists and workspace_id is not None and not is_global_owner:
            scope_col = "workspace_id"
            scope_val = workspace_id
        else:
            scope_col = None
            scope_val = None
        if status:
            if scope_col:
                cur.execute(
                    f"SELECT * FROM approvals WHERE status=%s AND {scope_col}=%s ORDER BY created_at DESC LIMIT %s",
                    (status, scope_val, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM approvals WHERE status=%s ORDER BY created_at DESC LIMIT %s",
                    (status, limit),
                )
        else:
            if scope_col:
                cur.execute(
                    f"SELECT * FROM approvals WHERE {scope_col}=%s ORDER BY created_at DESC LIMIT %s",
                    (scope_val, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM approvals ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
        return cur.fetchall()


# ── DB helper ─────────────────────────────────────────────────────────────────

def _get_approval(token: str):
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM approvals WHERE token=%s", (token,))
            return cur.fetchone()
    except (AttributeError, TypeError, RuntimeError, OSError) as exc:
        log.error("approvals: DB lookup failed — %s", exc)
        return None
