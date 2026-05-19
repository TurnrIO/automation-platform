"""IMAP email trigger — polls an inbox for new/matching messages.

Credential fields expected (store as a credential with these keys):
  host       — IMAP server hostname (e.g. imap.gmail.com)
  port       — port, default 993
  username   — email address / login
  password   — password or app-password
  use_ssl    — "true" (default) or "false"

Output shape
------------
{
  "emails":  [ {message_id, subject, from, to, date, body, html_body, attachment_names}, … ],
  "count":   N,
  # first-email shortcut fields (top-level) when at least one message was fetched:
  "message_id", "subject", "from", "to", "date", "body", "html_body", "attachment_names"
}
"""
import email as _email_module
import email.header as _email_header
import imaplib
import re as _re
import logging
from ._utils import _render, _safe_eval

logger = logging.getLogger(__name__)

NODE_TYPE = "trigger.email"
LABEL     = "Email Trigger (IMAP)"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _decode_header(value: str) -> str:
    """Decode RFC 2047 encoded email headers to a plain string."""
    if not value:
        return ""
    parts = _email_header.decode_header(value)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(str(part))
    return " ".join(decoded).strip()


def _get_body(msg) -> tuple:
    """Return (plain_text, html_text) from a parsed email.Message."""
    plain, html = "", ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype  = part.get_content_type()
            cdispo = str(part.get("Content-Disposition", ""))
            if "attachment" in cdispo:
                continue
            charset = part.get_content_charset() or "utf-8"
            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            text = payload.decode(charset, errors="replace")
            if ctype == "text/plain" and not plain:
                plain = text
            elif ctype == "text/html" and not html:
                html = text
    else:
        charset = msg.get_content_charset() or "utf-8"
        payload = msg.get_payload(decode=True)
        text = payload.decode(charset, errors="replace") if payload else ""
        if msg.get_content_type() == "text/html":
            html = text
        else:
            plain = text
    return plain, html


def _get_attachment_names(msg) -> list:
    names = []
    if msg.is_multipart():
        for part in msg.walk():
            cdispo = str(part.get("Content-Disposition", ""))
            if "attachment" in cdispo:
                fname = part.get_filename()
                if fname:
                    names.append(_decode_header(fname))
    return names


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    logger.info("[trigger.email] Starting email trigger run")
    creds = creds or {}

    # Credential lookup
    cred_name = _render(config.get("credential", ""), context, creds)
    cred      = creds.get(cred_name, {})

    # Connection parameters — config overrides credential fields
    host     = _render(config.get("host",     cred.get("host",     "")), context, creds).strip()
    port_raw = _render(config.get("port",     str(cred.get("port", "993"))), context, creds)
    username = _render(config.get("username", cred.get("username", "")), context, creds).strip()
    password = _render(config.get("password", cred.get("password", "")), context, creds)
    use_ssl_raw = str(cred.get("use_ssl", config.get("use_ssl", "true"))).lower()
    use_ssl  = use_ssl_raw not in ("false", "0", "no")

    # Behaviour parameters
    folder          = _render(config.get("folder",          "INBOX"),  context, creds).strip() or "INBOX"
    search_criteria = _render(config.get("search_criteria", "UNSEEN"), context, creds).strip() or "UNSEEN"
    filter_expr     = _render(config.get("filter_expression", ""),     context, creds).strip()
    max_msg_raw     = _render(config.get("max_messages", "10"),        context, creds)
    mark_read       = str(config.get("mark_read", "false")).lower() in ("true", "1", "yes")

    try:
        port = int(port_raw)
    except (ValueError, TypeError):
        port = 993

    try:
        max_msg = max(1, int(max_msg_raw))
    except (ValueError, TypeError):
        max_msg = 10

    if not host or not username:
        raise ValueError(
            "trigger.email: IMAP credential must include at least 'host' and 'username'"
        )

    logger.info(
        "[trigger.email] Connecting to %s:%s (%s) as %s",
        host, port,
        "SSL" if use_ssl else "plain",
        username,
    )

    try:
        conn = imaplib.IMAP4_SSL(host, port) if use_ssl else imaplib.IMAP4(host, port)
    except OSError as exc:
        logger.warning("[trigger.email] Connection failed — %s:%s — %s", host, port, exc)
        return {"__error": f"Email trigger: could not connect to {host}:{port} — {exc}", "emails": [], "count": 0}

    try:
        conn.login(username, password)
    except imaplib.IMAP4.error as exc:
        logger.warning("[trigger.email] Login failed for %s@%s — %s", username, host, exc)
        try:
            conn.logout()
        except (AttributeError, TypeError, OSError):
            pass
        return {"__error": f"Email trigger: login failed for {username} — {exc}", "emails": [], "count": 0}
    except OSError as exc:
        logger.warning("[trigger.email] Connection error during login — %s:%s — %s", host, port, exc)
        try:
            conn.logout()
        except (AttributeError, TypeError, OSError):
            pass
        return {"__error": f"Email trigger: connection error during login — {exc}", "emails": [], "count": 0}

    # — select folder + search + fetch (runs on success OR after OSError during login) —
    try:
        conn.select(folder, readonly=not mark_read)
    except imaplib.IMAP4.error as exc:
        logger.warning("[trigger.email] Select folder failed — %s — %s", folder, exc)
        try:
            conn.logout()
        except (AttributeError, TypeError, OSError):
            pass
        return {"__error": f"Email trigger: could not select folder '{folder}' — {exc}", "emails": [], "count": 0}
    except OSError as exc:
        logger.warning("[trigger.email] OS error selecting folder — %s — %s", folder, exc)
        return {"__error": f"Email trigger: OS error selecting folder '{folder}' — {exc}", "emails": [], "count": 0}

    typ, data = conn.search(None, search_criteria)
    if typ != "OK":
        raise RuntimeError(f"IMAP SEARCH failed: {typ} {data}")

    all_ids = data[0].split() if data and data[0] else []
    # Take the most-recent N message IDs
    ids = all_ids[-max_msg:]

    emails = []
    for uid in ids:
        typ2, raw = conn.fetch(uid, "(RFC822)")
        if typ2 != "OK" or not raw or raw[0] is None:
            continue
        raw_bytes = raw[0][1] if isinstance(raw[0], tuple) else raw[0]
        if not isinstance(raw_bytes, bytes):
            continue
        msg = _email_module.message_from_bytes(raw_bytes)

        plain, html = _get_body(msg)
        attachments = _get_attachment_names(msg)

        entry = {
            "message_id":       msg.get("Message-ID", "").strip(),
            "subject":          _decode_header(msg.get("Subject", "")),
            "from":             _decode_header(msg.get("From", "")),
            "to":               _decode_header(msg.get("To", "")),
            "date":             msg.get("Date", ""),
            "body":             plain,
            "html_body":        html,
            "attachment_names": attachments,
        }

        if filter_expr:
            try:
                keep = _safe_eval(filter_expr, {"email": entry, "re": _re})
                if not keep:
                    continue
            except (SyntaxError, ValueError, NameError, TypeError) as exc:
                logger.info("[trigger.email] Filter expression error: %s — skipping message", exc)
                continue

        emails.append(entry)

        if mark_read:
            conn.store(uid, "+FLAGS", "\Seen")

    logger.info("[trigger.email] Fetched %s message(s) from %s", len(emails), folder)

    result = {"emails": emails, "count": len(emails)}
    if emails:
        result.update(emails[0])
    return result
    finally:
        try:
            conn.logout()
        except (AttributeError, TypeError, OSError):
            pass
