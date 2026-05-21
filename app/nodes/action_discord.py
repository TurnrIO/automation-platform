"""Discord webhook action node."""
import json
import ipaddress
import logging
import socket
import urllib.parse
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.discord"
LABEL = "Discord"

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

logger = logging.getLogger(__name__)


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
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise ValueError(
            f"Discord: only https:// URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Discord: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Discord: could not resolve hostname: {host}")
    for family, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Discord: URL resolves to blocked IP {ip_str}. "
                f"URL: {url[:100]}"
            )


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send a message to a Discord channel via an Incoming Webhook."""
    import httpx

    logger.info("Discord: op=send")
    webhook_url = _render(config.get("webhook_url", ""), context, creds)
    message     = _render(config.get("message", ""),     context, creds)
    username    = _render(config.get("username", ""),    context, creds)
    avatar_url  = _render(config.get("avatar_url", ""),  context, creds)

    # Structured credential shortcut
    cred_name = _render(config.get("credential", ""), context, creds)
    if cred_name and creds and not webhook_url:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                webhook_url = c.get("webhook_url", "") or c.get("url", "")
            except (JSONDecodeError, AttributeError):
                webhook_url = raw  # fallback: raw value is the URL

    if not webhook_url:
        raise ValueError("Discord: no webhook_url configured")
    _check_url_ssrf(webhook_url)
    if not message:
        raise ValueError("Discord: no message configured")

    body: dict = {"content": message}
    if username:
        body["username"] = username
    if avatar_url:
        body["avatar_url"] = avatar_url

    try:
        r = httpx.post(webhook_url, json=body, timeout=10)
        r.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Discord: HTTP error — %s", exc)
        return {"__error": f"Discord HTTP error: {exc}", "sent": False}
    except OSError as exc:
        logger.warning("Discord: connection error — %s", exc)
        return {"__error": f"Discord connection error: {exc}", "sent": False}

    logger.info("Discord: sent message", extra={"chars": len(message)})
    logger.info("Discord: completion")
    return {"sent": True, "message": message}
