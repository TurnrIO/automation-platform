"""GraphQL action node.

Sends a GraphQL query or mutation to any endpoint.

Credential JSON fields (store as a generic/API Key credential):
  endpoint  — full GraphQL URL, e.g. https://api.example.com/graphql
  token     — Bearer token (added as Authorization: Bearer <token>)
  headers   — JSON string of extra headers (optional)


All config fields support {{template}} rendering.
"""
import ipaddress
import logging
import json
import socket
import urllib.parse
from json import JSONDecodeError

from app.nodes._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.graphql"
LABEL = "GraphQL"

logger = logging.getLogger(__name__)

# ── SSRF protection (same pattern as action_http_request) ─────────────────────

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
_ALLOWED_SCHEME = "https"


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


def _check_ssrf(url: str) -> None:
    """Validate URL scheme and resolve hostname for SSRF check.
    Raises ValueError if URL is unsafe (non-HTTPS or resolves to blocked IP).
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != _ALLOWED_SCHEME:
        raise ValueError(
            f"GraphQL: only {_ALLOWED_SCHEME} URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"GraphQL: URL has no valid hostname: {url[:100]}")
    try:
        infos = socket.getaddrinfo(host, 443 if scheme == "https" else 80,
                                   socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError(f"GraphQL: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _socktype, _proto, _, sockaddr) in infos:
        if family in (socket.AF_INET, socket.AF_INET6):
            ip_str = sockaddr[0]
            if _blocked_ip(ip_str):
                raise ValueError(
                    f"GraphQL: URL resolves to blocked IP {ip_str}. "
                    f"Hostname '{host}' is not allowed. URL: {url[:100]}"
                )


def _parse_json_field(raw, label):
    """Parse a JSON string field, returning a dict/list or raising ValueError."""
    if not raw or not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except (JSONDecodeError, ValueError) as exc:
        raise ValueError(f"GraphQL: {label} is not valid JSON — {exc}") from exc


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute a GraphQL query or mutation."""
    import httpx

    logger.info("GraphQL: node started")

    # ── Resolve credential ────────────────────────────────────────────────────
    endpoint = _render(config.get("endpoint", ""), context, creds).strip()
    token    = _render(config.get("token", ""),    context, creds).strip()
    extra_headers_raw = _render(config.get("headers_json", ""), context, creds).strip()

    cred_name = _render(config.get("credential", ""), context, creds).strip()
    if cred_name and creds:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                endpoint = endpoint or c.get("endpoint", "") or c.get("url", "")
                token    = token    or c.get("token", "")    or c.get("api_key", "")
                if not extra_headers_raw:
                    extra_headers_raw = c.get("headers", "") or ""
            except (JSONDecodeError, AttributeError):
                # raw value might be a bare token
                token = token or raw.strip()

    if not endpoint:
        raise ValueError("GraphQL: 'endpoint' is required (set in config or credential)")

    # ── Build query & variables ───────────────────────────────────────────────
    query     = _render(config.get("query", ""), context, creds).strip()
    variables_raw = _render(config.get("variables_json", ""), context, creds).strip()
    op_name   = _render(config.get("operation_name", ""), context, creds).strip()
    try: timeout = float(_render(config.get("timeout", "30"), context, creds))
    except (ValueError, TypeError): timeout = 30.0

    if not query:
        raise ValueError("GraphQL: 'query' is required")

    variables = _parse_json_field(variables_raw, "variables_json") if variables_raw else {}
    extra_headers = _parse_json_field(extra_headers_raw, "headers_json") if extra_headers_raw else {}

    # ── Build headers ─────────────────────────────────────────────────────────
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    headers.update(extra_headers)

    # ── Build payload ─────────────────────────────────────────────────────────
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    if op_name:
        payload["operationName"] = op_name

    # ── SSRF check on endpoint ──────────────────────────────────────────────
    try:
        _check_ssrf(endpoint)
    except ValueError as exc:
        logger.warning("GraphQL: SSRF check failed — %s", exc)
        return {"__error": f"GraphQL: SSRF check failed: {exc}", "endpoint": endpoint}

    logger.info("GraphQL: executing query", extra={"endpoint": endpoint})
    try:
        resp = httpx.post(endpoint, json=payload, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("GraphQL: HTTP error — %s", exc)
        return {"__error": f"GraphQL HTTP error: {exc}", "endpoint": endpoint}
    except OSError as exc:
        logger.warning("GraphQL: connection error — %s", exc)
        return {"__error": f"GraphQL connection error: {exc}", "endpoint": endpoint}
    except (KeyError, IndexError, TypeError) as exc:
        logger.warning("GraphQL: unexpected response structure — %s", exc)
        return {"__error": f"GraphQL response structure error: {exc}", "endpoint": endpoint}

    # GraphQL servers typically return 200 even for errors; parse body first
    try:
        body = resp.json()
    except JSONDecodeError:
        resp.raise_for_status()
        raise ValueError(f"GraphQL: non-JSON response (status {resp.status_code})") from None

    data   = body.get("data")
    errors = body.get("errors", [])

    logger.info("GraphQL: query complete status=%s has_errors=%s", resp.status_code, bool(errors))

    if errors:
        messages = [e.get("message", str(e)) for e in errors]
        logger.warning("GraphQL errors: %s", messages)

    ignore_errors = str(config.get("ignore_errors", "false")).lower() == "true"
    if errors and not data and not ignore_errors:
        raise ValueError(f"GraphQL errors: {'; '.join(messages)}")

    return {
        "data":        data,
        "errors":      errors,
        "has_errors":  bool(errors),
        "status_code": resp.status_code,
        "ok":          resp.status_code < 400 and not (errors and not data),
    }
