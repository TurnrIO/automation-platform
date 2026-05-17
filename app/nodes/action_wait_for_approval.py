"""action.wait_for_approval — Human-in-the-loop approval gate.

Sends an email to the configured approver with Approve / Reject buttons.
The node blocks (polls Redis every 10 s) until a decision is made or
the timeout elapses.

Config fields
─────────────
approver_email  Recipient of the approval request (required; supports {{}} templates)
subject         Email subject  (default: "Action required: approval needed")
message         Body text / context shown to the approver  (supports templates)
timeout_hours   How long to wait before failing  (default 48, max 168 / 7 days)

Output
──────
{
  "approved":   true | false,
  "decision":   "approved" | "rejected",
  "token":      "<hex token>",
  "approve_url": "<url>",   # useful to surface in prior notification nodes
  "reject_url":  "<url>",
}
"""
import uuid
import time
import os
import logging

import psycopg2

from app.nodes._utils import _render

logger = logging.getLogger(__name__)

NODE_TYPE = "action.wait_for_approval"
LABEL     = "Wait for Approval"

_POLL_INTERVAL = 10   # seconds between Redis checks
_MAX_HOURS     = 168  # 7 days hard cap


def run(config, inp, context, logger, creds=None, **kwargs):
    approver_email = _render(config.get("approver_email", ""), context, creds).strip()
    if not approver_email:
        raise ValueError("approver_email is required")

    subject       = _render(config.get("subject",  "Action required: approval needed"), context, creds)
    message       = _render(config.get("message",  ""), context, creds)
    try: timeout_hours = int(_render(str(config.get("timeout_hours", 48)), context, creds) or 48)
    except (ValueError, TypeError): timeout_hours = 48
    timeout_hours = min(timeout_hours, _MAX_HOURS)

    token       = uuid.uuid4().hex           # 32-char hex, no dashes
    task_id     = str(context.get("__task_id", "") or "")
    graph_name  = str(context.get("__graph_name", "") or "")
    node_id     = str(kwargs.get("node_id", "") or "")
    app_url     = os.environ.get("APP_URL", "http://localhost").rstrip("/")

    approve_url = f"{app_url}/api/approvals/{token}/approve"
    reject_url  = f"{app_url}/api/approvals/{token}/reject"

    # ── Persist approval record ──────────────────────────────────────────────
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO approvals
                   (token, task_id, graph_name, node_id, approver_email,
                    subject, message, timeout_hours, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'pending')""",
                    (token, task_id, graph_name, node_id,
                     approver_email, subject, message, timeout_hours),
                )
    except psycopg2.Error as exc:
        logger.warning("wait_for_approval: could not persist record — %s", exc)

    # ── Send email ───────────────────────────────────────────────────────────
    _send_approval_email(approver_email, subject, message, approve_url, reject_url,
                         graph_name, app_url)

    logger.info(
        "Approval requested from %s (token=%s…), waiting up to %dh",
        approver_email, token[:8], timeout_hours,
    )

    # ── Poll Redis for decision ──────────────────────────────────────────────
    redis_url  = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    redis_key  = f"approval:{token}:decision"
    timeout_s  = timeout_hours * 3600
    start      = time.time()

    try:
        import redis as _redis
        r = _redis.from_url(redis_url, socket_connect_timeout=5)
        # Mark as pending so the approve endpoint can validate the token
        r.setex(f"approval:{token}:pending", timeout_s + 3600, "1")
    except (OSError, RuntimeError) as exc:
        raise RuntimeError(f"Cannot connect to Redis for approval polling: {exc}") from exc

    while True:
        try:
            raw = r.get(redis_key)
        except (ConnectionError, TimeoutError, OSError):
            raw = None

        if raw:
            decision = raw.decode()
            _update_status(token, decision)
            logger.info("Approval decision received: %s", decision)
            return {
                "approved":    decision == "approved",
                "decision":    decision,
                "token":       token,
                "approve_url": approve_url,
                "reject_url":  reject_url,
            }

        elapsed = time.time() - start
        if elapsed >= timeout_s:
            _update_status(token, "expired")
            raise TimeoutError(
                f"Approval timed out after {timeout_hours} hour(s). "
                f"Token: {token[:8]}…"
            )

        time.sleep(_POLL_INTERVAL)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_status(token: str, status: str) -> None:
    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE approvals SET status=%s, decided_at=NOW() WHERE token=%s AND status='pending'",
                    (status, token),
                )
    except psycopg2.Error as exc:
        logger.warning("wait_for_approval: could not update status — %s", exc)


def _send_approval_email(
    to: str, subject: str, message: str,
    approve_url: str, reject_url: str,
    graph_name: str, app_url: str,
) -> None:
    msg_block = ""
    if message:
        import html as _html
        msg_block = f"""
        <div style="background:#1e2130;border-left:3px solid #7c3aed;padding:14px 18px;
                    border-radius:6px;margin:20px 0;font-size:14px;color:#cbd5e1;
                    white-space:pre-wrap;">{_html.escape(message)}</div>"""

    flow_label = f" for <strong style='color:#e2e8f0;'>{graph_name}</strong>" if graph_name else ""

    html = f"""
<div style="background:#0d0f1a;min-height:100vh;padding:40px 20px;
            font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:520px;margin:0 auto;background:#13151f;
              border:1px solid #1e2130;border-radius:16px;overflow:hidden;">
    <div style="background:linear-gradient(135deg,#7c3aed,#6d28d9);padding:24px 32px;">
      <div style="color:#fff;font-size:20px;font-weight:700;letter-spacing:-.3px;">
        ⚡ HiveRunr
      </div>
    </div>
    <div style="padding:32px;">
      <div style="font-size:28px;margin-bottom:8px;">🔔</div>
      <h2 style="color:#e2e8f0;font-size:18px;font-weight:600;margin:0 0 6px;">
        Your approval is needed
      </h2>
      <p style="color:#64748b;font-size:14px;margin:0 0 4px;">
        A workflow{flow_label} has paused and is waiting for your decision.
      </p>
      {msg_block}
      <div style="display:flex;gap:12px;margin-top:24px;flex-wrap:wrap;">
        <a href="{approve_url}"
           style="flex:1;min-width:130px;text-align:center;padding:12px 20px;
                  background:linear-gradient(135deg,#16a34a,#15803d);
                  color:#fff;text-decoration:none;border-radius:8px;
                  font-size:14px;font-weight:700;letter-spacing:.3px;">
          ✅ Approve
        </a>
        <a href="{reject_url}"
           style="flex:1;min-width:130px;text-align:center;padding:12px 20px;
                  background:linear-gradient(135deg,#dc2626,#b91c1c);
                  color:#fff;text-decoration:none;border-radius:8px;
                  font-size:14px;font-weight:700;letter-spacing:.3px;">
          ❌ Reject
        </a>
      </div>
      <p style="color:#374151;font-size:12px;margin:20px 0 0;line-height:1.6;">
        Clicking either button will record your decision and allow the workflow
        to continue. Each link can only be used once.
      </p>
    </div>
    <div style="padding:16px 32px;border-top:1px solid #1e2130;
                color:#374151;font-size:11px;">
      HiveRunr approval request ·
      <a href="{app_url}" style="color:#7c3aed;">Open dashboard</a>
    </div>
  </div>
</div>"""

    try:
        from app.email import send_email
        ok = send_email(to, subject, html)
        if not ok:
            logger.warning("wait_for_approval: email not sent (not configured?). "
                        "Approve: %s  Reject: %s", approve_url, reject_url)
    except (OSError, AttributeError) as exc:
        logger.warning("wait_for_approval: email send failed — %s. "
                    "Approve: %s  Reject: %s", exc, approve_url, reject_url)
