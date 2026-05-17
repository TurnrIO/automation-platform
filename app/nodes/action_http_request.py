"""HTTP request action node."""
import ipaddress
import logging
import json
import socket
import urllib.parse
from json import JSONDecodeError

import httpx
from app.nodes._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.http_request"
LABEL = "HTTP Request"

logger = logging.getLogger(__name__)

# ── SSRF protection ────────────────────────────────────────────────────────────

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),      # loopback
    ipaddress.ip_network("169.254.0.0/16"),   # link-local / AWS IMDS
    ipaddress.ip_network("10.0.0.0/8"),       # RFC1918 private
    ipaddress.ip_network("172.16.0.0/12"),    # RFC1918 private
    ipaddress.ip_network("192.168.0.0/16"),   # RFC1918 private
    ipaddress.ip_network("0.0.0.0/8"),         # current network
    ipaddress.ip_network("224.0.0.0/4"),       # multicast
    ipaddress.ip_network("::1/128"),           # loopback IPv6
    ipaddress.ip_network("fe80::/10"),         # link-local IPv6
    ipaddress.ip_network("ff00::/8"),          # multicast IPv6
]
# AWS EC2 metadata endpoint — always block even if DNS resolves to it
_IMDS_IP = ipaddress.ip_address("169.254.169.254")
_ALLOWED_SCHEME = "https"


def _blocked_ip(ip_str: str) -> bool:
    """Return True if ip_str is in a blocked network range."""
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
    Raises ValueError if the URL is unsafe (non-HTTPS or resolves to blocked IP).
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme != _ALLOWED_SCHEME:
        raise ValueError(
            f"HTTP Request: only {_ALLOWED_SCHEME} URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )

    host = parsed.hostname
    if not host:
        raise ValueError(f"HTTP Request: URL has no valid hostname: {url[:100]}")

    # Resolve hostname to IP(s) and check each against blocked ranges
    try:
        infos = socket.getaddrinfo(host, 443 if scheme == "https" else 80, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError(f"HTTP Request: could not resolve hostname '{host}' in URL: {url[:100]}")

    for (family, _socktype, _proto, _, sockaddr) in infos:
        if family == socket.AF_INET:
            ip_str = sockaddr[0]
        elif family == socket.AF_INET6:
            ip_str = sockaddr[0]
        else:
            continue
        if _blocked_ip(ip_str):
            raise ValueError(
                f"HTTP Request: URL resolves to blocked IP {ip_str}. "
                f"Hostname '{host}' is not allowed. URL: {url[:100]}"
            )


# ── main node logic ────────────────────────────────────────────────────────────

def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute HTTP request and return response."""
    import urllib.parse

    url     = _render(config.get("url", ""),    context, creds)
    method  = config.get("method", "GET").upper()
    headers = {}

    # ── Credential shortcut (Bearer token / API key) ──────────────────────
    cred_name = _render(config.get("credential", ""), context, creds)
    if cred_name and creds:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                # Support {token}, {api_key}, or {Authorization} fields
                token = c.get("token") or c.get("api_key") or c.get("Authorization")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
            except (JSONDecodeError, AttributeError):
                # Raw string — treat as a Bearer token directly
                headers["Authorization"] = f"Bearer {raw}"

    # ── Headers ───────────────────────────────────────────────────────────
    if config.get("headers_json"):
        try:
            extra = json.loads(_render(config["headers_json"], context, creds))
            headers.update(extra)
        except (JSONDecodeError, ValueError):
            pass

    # ── Body (JSON) ───────────────────────────────────────────────────────
    json_body  = None
    raw_body   = None
    form_body  = None

    if config.get("body_json"):
        rendered = _render(config["body_json"], context, creds)
        try:
            parsed = json.loads(rendered)
            json_body = parsed if isinstance(parsed, dict) else None
            raw_body  = rendered if not isinstance(parsed, dict) else None
        except (JSONDecodeError, ValueError):
            raw_body = rendered

    # ── Form data ─────────────────────────────────────────────────────────
    if config.get("form_json"):
        try:
            form_body = json.loads(_render(config["form_json"], context, creds))
        except (JSONDecodeError, ValueError):
            pass

    # ── Timeout ───────────────────────────────────────────────────────────
    try:
        timeout = float(_render(str(config.get("timeout") or "30"), context, creds) or 30)
    except (ValueError, TypeError):
        timeout = 30

    # ── SSRF validation on initial URL ────────────────────────────────────
    if not url:
        raise ValueError("HTTP Request: no URL configured")

    _check_url_ssrf(url)

    # ── Execute with redirect validation ─────────────────────────────────
    logger.info("HTTP %s %s", method, url)

    client = httpx.Client(timeout=timeout, follow_redirects=False)
    current_url = url
    response_history = []

    try:
        while True:
            try:
                r = client.request(
                    method, current_url,
                    headers=headers,
                    json=json_body,
                    content=raw_body.encode() if isinstance(raw_body, str) else None,
                    data=form_body,
                )
            except (httpx.HTTPError, OSError) as exc:
                logger.warning("HTTP %s %s: connection error — %s", method, current_url, exc)
                return {"__error": f"HTTP request failed: {exc}"}

            # If not a redirect, we're done
            if not r.is_redirect:
                break

            location = r.headers.get("location", "")
            if not location:
                break  # 204 No Location is still a terminal response

            response_history.append(r)
            logger.info("HTTP %s → %s → redirect to %s", current_url, r.status_code, location)

            # Resolve relative Location against current URL
            resolved = urllib.parse.urljoin(current_url, location)

            # Check redirect URL for SSRF
            try:
                _check_url_ssrf(resolved)
            except ValueError as exc:
                logger.warning("HTTP %s: redirect SSRF check failed — %s", current_url, exc)
                return {"__error": f"HTTP redirect SSRF check failed: {exc}"}

            current_url = resolved

        # Handle non-2xx status codes
        ignore_errors = str(config.get("ignore_errors", "false")).lower() == "true"
        if not ignore_errors:
            r.raise_for_status()

        try:
            rbody = r.json()
        except (JSONDecodeError, ValueError):
            rbody = r.text

        ok = 200 <= r.status_code < 300
        logger.info("HTTP %s %s → %s", method, r.url, r.status_code)

        return {
            "status":  r.status_code,
            "ok":      ok,
            "body":    rbody,
            "headers": dict(r.headers),
            "redirects": [resp.url for resp in response_history],
        }
    finally:
        client.close()
