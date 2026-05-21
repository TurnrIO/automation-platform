"""Slack message action node."""
import logging
import json
import ipaddress
import socket
import urllib.parse
import httpx
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)
NODE_TYPE = "action.slack"
LABEL = "Slack"

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
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise ValueError(
            f"Slack: only https:// URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Slack: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Slack: could not resolve hostname: {host}")
    for family, _, _, _, sockaddr in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Slack: URL resolves to blocked IP {ip_str}. "
                f"URL: {url[:100]}"
            )


def run(config, inp, context, logger, creds=None, **kwargs):
    """Send message to Slack webhook."""
    logger.info("Slack: op=send")
    webhook_url = _render(config.get('webhook_url', ''), context, creds)
    message = _render(config.get('message', ''), context, creds)
    channel = _render(config.get('channel', ''), context, creds)

    # Structured credential shortcut (Slack or generic Webhook type)
    cred_name = _render(config.get('credential', ''), context, creds)
    if cred_name and creds and not webhook_url:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                webhook_url = c.get('webhook_url', '') or c.get('url', '')
            except (JSONDecodeError, AttributeError):
                webhook_url = raw  # fallback: raw value is the URL

    if not webhook_url:
        raise ValueError("Slack: no webhook_url configured")
    try:
        _check_url_ssrf(webhook_url)
    except ValueError as exc:
        logger.warning("Slack: SSRF check failed — %s", exc)
        return {"__error": f"Slack SSRF check failed: {exc}", "sent": False}
    if not message:
        raise ValueError("Slack: no message configured")
    logger.info("Slack: sending message len=%s", len(message))

    body = {'text': message}
    if channel:
        body['channel'] = channel

    try:
        r = httpx.post(webhook_url, json=body, timeout=10)
        r.raise_for_status()
    except (httpx.HTTPError, OSError) as exc:
        logger.warning("Slack: HTTP error — %s", exc)
        return {"__error": f"Slack API call failed: HTTP error — {exc}", "sent": False}
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        logger.warning("Slack: unexpected error — %s", exc)
        return {"__error": f"Slack API call failed: {exc}", "sent": False}

    logger.info("Slack: message sent successfully", extra={"chars": len(message)})
    return {"sent": True, "message": message}
