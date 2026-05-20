"""Twilio SMS / WhatsApp / Voice REST API node."""
import logging
import base64
import ipaddress
import json
import socket
import urllib.parse
import urllib.request
import urllib.error
from json import JSONDecodeError
from app.nodes._utils import _render

logger = logging.getLogger(__name__)
NODE_TYPE = "action.twilio"
LABEL     = "Twilio"

_API_BASE = "https://api.twilio.com/2010-04-01"

# ── SSRF protection ────────────────────────────────────────────────────────────

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _blocked_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        for net in _BLOCKED_NETWORKS:
            if ip in net:
                return True
    except ValueError:
        pass
    return False


def _check_url_ssrf(url: str) -> None:
    """Validate URL scheme and resolve hostname for SSRF check.
    Raises ValueError if URL is unsafe (non-HTTPS or resolves to blocked IP).
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise ValueError(
            f"Twilio: only https:// URLs are allowed for twiml_url. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Twilio: could not determine hostname from twiml_url: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Twilio: could not resolve hostname: {host}")
    for family, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Twilio: twiml_url resolves to blocked IP {ip_str}. "
                f"URL: {url[:100]}"
            )

def _req(method, path, account_sid, auth_token, body=None):
    url   = f"{_API_BASE}/Accounts/{account_sid}{path}"
    data  = urllib.parse.urlencode(body).encode() if body else None
    req   = urllib.request.Request(url, data=data, method=method)
    creds = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
    req.add_header("Authorization", f"Basic {creds}")
    if data:
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        try:    detail = json.loads(body_txt).get("message", body_txt)
        except JSONDecodeError: detail = body_txt
        raise RuntimeError(f"Twilio {e.code}: {detail}")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Twilio URL error: {exc}") from exc
    except OSError as exc:
        raise RuntimeError(f"Twilio connection error: {exc}") from exc
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"Twilio response error: {exc}") from exc

def run(config, inp, context, logger, creds=None, **kwargs):
    logger.info("Twilio: run() called")
    # ── resolve credentials ────────────────────────────────────────────────
    cred_name   = config.get("credential", "")
    account_sid = ""
    auth_token  = ""
    if cred_name and creds:
        raw = creds.get(cred_name, {})
        if isinstance(raw, str):
            try:   raw = json.loads(raw)
            except JSONDecodeError: raw = {}
        account_sid = raw.get("account_sid", "")
        auth_token  = raw.get("auth_token", "")
    if not account_sid:
        account_sid = _render(config.get("account_sid", ""), context, creds)
    if not auth_token:
        auth_token  = _render(config.get("auth_token", ""), context, creds)
    op = _render(config.get("operation", "send_sms"), context, creds)
    logger.info("Twilio: op=%s", op)

    # ── credential validation ───────────────────────────────────────────────
    if not account_sid or not auth_token:
        raise ValueError("Twilio: account_sid and auth_token are required")

    # ── send SMS ───────────────────────────────────────────────────────────
    if op in ("send_sms", "send_whatsapp"):
        logger.info("Twilio: sending %s", op)
        to_   = _render(config.get("to", ""), context, creds)
        from_ = _render(config.get("from", ""), context, creds)
        body_ = _render(config.get("body", ""), context, creds)
        if op == "send_whatsapp":
            if not to_.startswith("whatsapp:"):    to_   = f"whatsapp:{to_}"
            if not from_.startswith("whatsapp:"): from_ = f"whatsapp:{from_}"
        result = _req("POST", "/Messages.json", account_sid, auth_token, {
            "To": to_, "From": from_, "Body": body_,
        })
        if isinstance(result, dict) and "__error" in result:
            return result
        sid = result.get("sid", "unknown")
        status_ = result.get("status", "unknown")
        logger.info("Twilio %s: sid=%s status=%s to=%s", op, sid, status_, to_)
        return {
            "sid":    result.get("sid"),
            "status": result.get("status"),
            "to":     result.get("to"),
            "from":   result.get("from"),
            "body":   result.get("body"),
            "error_code": result.get("error_code"),
            "raw":    result,
        }

    # ── make call ────────────────────────────────────────────────────────────
    elif op == "make_call":
        to_    = _render(config.get("to", ""), context, creds)
        from_  = _render(config.get("from", ""), context, creds)
        url_   = _render(config.get("twiml_url", ""), context, creds)
        twiml_ = _render(config.get("twiml", ""), context, creds)
        logger.info("Twilio: make_call to=%s", to_)
        if not url_ and not twiml_:
            raise ValueError("Twilio make_call: twiml_url or twiml is required")
        params = {"To": to_, "From": from_}
        if url_:
            try:
                _check_url_ssrf(url_)
            except ValueError as e:
                logger.warning("Twilio make_call: SSRF check failed — %s", e)
                return {"__error": str(e), "url": url_}
            params["Url"] = url_
        else:
            params["Twiml"] = twiml_
        result = _req("POST", "/Calls.json", account_sid, auth_token, params)
        if isinstance(result, dict) and "__error" in result:
            return result
        sid = result.get("sid", "unknown")
        status_ = result.get("status", "unknown")
        logger.info("Twilio make_call: sid=%s status=%s to=%s", sid, status_, to_)
        return {
            "sid":    result.get("sid"),
            "status": result.get("status"),
            "to":     result.get("to"),
            "from":   result.get("from"),
            "raw":    result,
        }

    # ── check status ─────────────────────────────────────────────────────────
    elif op == "check_status":
        sid    = _render(config.get("sid", ""), context, creds)
        kind   = _render(config.get("resource_type", "message"), context, creds).lower()
        logger.info("Twilio: check_status sid=%s", sid)
        suffix = "/Messages" if kind == "message" else "/Calls"
        result = _req("GET", f"{suffix}/{sid}.json", account_sid, auth_token)
        if isinstance(result, dict) and "__error" in result:
            return result
        logger.info("Twilio check_status: sid=%s status=%s", sid, result.get("status", "unknown"))
        return {
            "sid":        result.get("sid"),
            "status":     result.get("status"),
            "to":         result.get("to"),
            "from":       result.get("from"),
            "body":       result.get("body"),
            "error_code": result.get("error_code"),
            "raw":        result,
        }

    # ── list messages ───────────────────────────────────────────────────────
    elif op == "list_messages":
        logger.info("Twilio: list_messages")
        to_   = _render(config.get("to", ""), context, creds)
        from_ = _render(config.get("from", ""), context, creds)
        try: limit = int(_render(config.get("limit", "20"), context, creds))
        except (ValueError, TypeError): limit = 20
        qs    = urllib.parse.urlencode({k: v for k, v in {"To": to_, "From": from_, "PageSize": limit}.items() if v})
        result = _req("GET", f"/Messages.json?{qs}", account_sid, auth_token)
        if isinstance(result, dict) and "__error" in result:
            return result
        msgs  = result.get("messages", [])
        logger.info("Twilio list_messages: count=%d to=%s from=%s", len(msgs), to_, from_)
        return {
            "messages": [{
                "sid": m.get("sid"), "status": m.get("status"),
                "to": m.get("to"), "from": m.get("from"),
                "body": m.get("body"), "date_sent": m.get("date_sent"),
            } for m in msgs],
            "count": len(msgs),
        }

    else:
        raise ValueError(f"Twilio: unknown operation {op!r}")
