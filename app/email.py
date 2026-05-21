"""AgentMail.to email helper for HiveRunr.

Configuration (via .env):
    AGENTMAIL_API_KEY  — API key from agentmail.to dashboard
    AGENTMAIL_FROM     — Sender inbox address (e.g. alerts@agentmail.to)
    APP_URL            — Base URL for links in emails (e.g. https://hiverunr.example.com)
    OWNER_EMAIL        — Owner's email for password-reset and system alerts

AgentMail API: https://docs.agentmail.to/api-reference/inboxes/messages/send
  POST https://api.agentmail.to/v0/inboxes/{inbox_id}/messages/send
  Authorization: Bearer {AGENTMAIL_API_KEY}
  Body: { "to": ["addr"], "subject": "...", "html": "...", "text": "..." }

IMPORTANT — inbox_id is the FULL from-address (e.g. "alerts@agentmail.to"),
NOT just the local part.  The /messages/send suffix is also required.
Using /messages (no /send) or just the local-part inbox_id returns 404/401.
"""
import os
import logging

log = logging.getLogger(__name__)

AGENTMAIL_BASE = "https://api.agentmail.to/v0"


def _is_configured() -> bool:
    return bool(os.environ.get("AGENTMAIL_API_KEY") and os.environ.get("AGENTMAIL_FROM"))


def send_email(to: "str | list[str]", subject: str, html: str, text: str = "") -> bool:
    """Send an email via AgentMail.to.

    Args:
        to:      Recipient address or list of addresses.
        subject: Email subject line.
        html:    HTML body.
        text:    Plain-text fallback body (auto-stripped from html if empty).

    Returns:
        True if the message was accepted, False on any error (never raises).
    """
    api_key  = os.environ.get("AGENTMAIL_API_KEY", "")
    from_addr = os.environ.get("AGENTMAIL_FROM", "")

    if not api_key or not from_addr:
        log.warning("email: AGENTMAIL_API_KEY / AGENTMAIL_FROM not configured — skipping")
        return False

    # NOTE: inbox_id must be the FULL from-address, not just the local-part.
    # e.g. "alerts@agentmail.to" not "alerts".  See module docstring.
    inbox_id = from_addr

    if isinstance(to, str):
        to = [t.strip() for t in to.split(",") if t.strip()]

    if not to:
        log.warning("email: no recipients — skipping")
        return False

    if not text:
        # Basic strip of HTML tags for the plain-text fallback
        import re
        text = re.sub(r"<[^>]+>", "", html).strip()

    payload = {
        "to":      to,
        "subject": subject,
        "html":    html,
        "text":    text,
    }

    try:
        import httpx
        resp = httpx.post(
            f"{AGENTMAIL_BASE}/inboxes/{inbox_id}/messages/send",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code >= 400:
            log.error("email: AgentMail returned %s — %s", resp.status_code, resp.text[:300])
            return False
        log.info("email: sent to %s (subject=%r)", to, subject)
        return True
    except (httpx.HTTPError, OSError) as exc:
        log.error("email: send failed — %s", exc)
        return False


# ── Pre-built email templates ─────────────────────────────────────────────────

def _app_url() -> str:
    return os.environ.get("APP_URL", "http://localhost").rstrip("/")


def send_run_alert(
    to: "str | list[str]",
    flow_name: str,
    status: str,
    task_id: str,
    error: str = "",
    graph_id: int = None,
) -> bool:
    """Send a flow run success/failure alert email."""
    colour  = "#22c55e" if status == "succeeded" else "#ef4444"
    icon    = "✅" if status == "succeeded" else "❌"
    label   = "succeeded" if status == "succeeded" else "failed"
    app_url = _app_url()
    runs_url = f"{app_url}/#logs"

    error_block = ""
    if error:
        error_block = f"""
        <div style="background:#1e1e2e;border-left:3px solid #ef4444;padding:12px 16px;
                    margin:16px 0;border-radius:4px;font-family:monospace;font-size:13px;
                    color:#fca5a5;white-space:pre-wrap;">{error[:1000]}</div>"""

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
          <div style="font-size:28px;margin-bottom:8px;">{icon}</div>
          <h2 style="color:#e2e8f0;font-size:18px;font-weight:600;margin:0 0 4px;">
            Flow {label}
          </h2>
          <p style="color:#64748b;font-size:14px;margin:0 0 24px;">
            <strong style="color:{colour};">{flow_name}</strong>
          </p>

          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr>
              <td style="color:#64748b;padding:6px 0;width:100px;">Status</td>
              <td style="color:{colour};font-weight:600;padding:6px 0;">{label.upper()}</td>
            </tr>
            <tr>
              <td style="color:#64748b;padding:6px 0;">Flow</td>
              <td style="color:#e2e8f0;padding:6px 0;">{flow_name}</td>
            </tr>
            <tr>
              <td style="color:#64748b;padding:6px 0;">Task ID</td>
              <td style="color:#94a3b8;font-family:monospace;font-size:12px;padding:6px 0;">
                {task_id}
              </td>
            </tr>
          </table>

          {error_block}

          <a href="{runs_url}"
             style="display:inline-block;margin-top:20px;padding:10px 20px;
                    background:linear-gradient(135deg,#7c3aed,#6d28d9);
                    color:#fff;text-decoration:none;border-radius:8px;
                    font-size:13px;font-weight:600;">
            View Runs →
          </a>
        </div>
        <div style="padding:16px 32px;border-top:1px solid #1e2130;
                    color:#374151;font-size:11px;">
          HiveRunr alert · <a href="{app_url}" style="color:#7c3aed;">Open dashboard</a>
        </div>
      </div>
    </div>"""

    subject = f"[HiveRunr] {icon} {flow_name} {label}"
    return send_email(to, subject, html)


def send_password_reset(to: str, reset_url: str, username: str) -> bool:
    """Send a password reset link to the owner."""
    app_url = _app_url()
    html = f"""
    <div style="background:#0d0f1a;min-height:100vh;padding:40px 20px;
                font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
      <div style="max-width:480px;margin:0 auto;background:#13151f;
                  border:1px solid #1e2130;border-radius:16px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#7c3aed,#6d28d9);padding:24px 32px;">
          <div style="color:#fff;font-size:20px;font-weight:700;">⚡ HiveRunr</div>
        </div>
        <div style="padding:32px;">
          <h2 style="color:#e2e8f0;font-size:18px;font-weight:600;margin:0 0 8px;">
            Reset your password
          </h2>
          <p style="color:#64748b;font-size:14px;margin:0 0 24px;">
            Hi <strong style="color:#e2e8f0;">{username}</strong>, click the button below
            to set a new password. This link expires in 1 hour.
          </p>
          <a href="{reset_url}"
             style="display:inline-block;padding:12px 28px;
                    background:linear-gradient(135deg,#7c3aed,#6d28d9);
                    color:#fff;text-decoration:none;border-radius:8px;
                    font-size:14px;font-weight:600;">
            Reset password →
          </a>
          <p style="color:#374151;font-size:12px;margin:24px 0 0;">
            If you didn't request this, you can safely ignore this email.
            The link will expire automatically.
          </p>
        </div>
        <div style="padding:16px 32px;border-top:1px solid #1e2130;
                    color:#374151;font-size:11px;">
          HiveRunr · <a href="{app_url}" style="color:#7c3aed;">Open dashboard</a>
        </div>
      </div>
    </div>"""

    return send_email(to, "Reset your HiveRunr password", html)


def send_flow_invite(
    to: str,
    invite_url: str,
    flow_name: str,
    role: str,
    invited_by: str,
) -> bool:
    """Send a flow access invite email."""
    app_url = _app_url()
    role_label = {"viewer": "Viewer", "runner": "Runner", "editor": "Editor"}.get(role, role)
    html = f"""
    <div style="background:#0d0f1a;min-height:100vh;padding:40px 20px;
                font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
      <div style="max-width:480px;margin:0 auto;background:#13151f;
                  border:1px solid #1e2130;border-radius:16px;overflow:hidden;">
        <div style="background:linear-gradient(135deg,#7c3aed,#6d28d9);padding:24px 32px;">
          <div style="color:#fff;font-size:20px;font-weight:700;">⚡ HiveRunr</div>
        </div>
        <div style="padding:32px;">
          <h2 style="color:#e2e8f0;font-size:18px;font-weight:600;margin:0 0 8px;">
            You've been invited to a flow
          </h2>
          <p style="color:#64748b;font-size:14px;margin:0 0 24px;">
            <strong style="color:#e2e8f0;">{invited_by}</strong> has granted you
            <strong style="color:#a78bfa;">{role_label}</strong> access to the flow
            <strong style="color:#e2e8f0;">{flow_name}</strong>.
          </p>
          <a href="{invite_url}"
             style="display:inline-block;padding:12px 28px;
                    background:linear-gradient(135deg,#7c3aed,#6d28d9);
                    color:#fff;text-decoration:none;border-radius:8px;
                    font-size:14px;font-weight:600;">
            Accept invitation →
          </a>
          <p style="color:#374151;font-size:12px;margin:24px 0 0;">
            This link expires in 7 days. If you didn't expect this invite, you can safely
            ignore this email.
          </p>
        </div>
        <div style="padding:16px 32px;border-top:1px solid #1e2130;
                    color:#374151;font-size:11px;">
          HiveRunr · <a href="{app_url}" style="color:#7c3aed;">Open dashboard</a>
        </div>
      </div>
    </div>"""

    return send_email(to, f"[HiveRunr] You've been invited to '{flow_name}'", html)
