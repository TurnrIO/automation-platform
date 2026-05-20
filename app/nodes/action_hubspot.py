"""HubSpot CRM REST API v3 node."""
import ipaddress
import json
import logging
import socket
from urllib import parse as urllib_parse

import httpx
from json import JSONDecodeError
from app.nodes._utils import _render

logger = logging.getLogger(__name__)
NODE_TYPE = "action.hubspot"
LABEL     = "HubSpot"

_BASE = "https://api.hubapi.com"

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
    parsed = urllib_parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"HubSpot: only http/https URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"HubSpot: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"HubSpot: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _, _, _, sockaddr) in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"HubSpot: URL resolves to blocked address {ip_str}. "
                f"URL: {url[:100]}"
            )


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _req(method, path, token, body=None):
    url  = _BASE + path
    try:
        _check_url_ssrf(url)
    except ValueError as e:
        raise RuntimeError(f"HubSpot SSRF check failed: {e}")

    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    max_redirects = 10
    while max_redirects > 0:
        try:
            r = httpx.request(method, url, headers=headers, content=data,
                              timeout=httpx.Timeout(15.0), follow_redirects=False)
        except httpx.HTTPStatusError as e:
            body_txt = e.response.text
            try:    detail = json.loads(body_txt).get("message", body_txt)
            except JSONDecodeError: detail = body_txt
            logger.warning("HubSpot: HTTP error — %s %s", e.response.status_code, detail)
            raise RuntimeError(f"HubSpot HTTP {e.response.status_code}: {detail}")
        except httpx.HTTPError as e:
            logger.warning("HubSpot: HTTP error — %s", e)
            raise RuntimeError(f"HubSpot HTTP error: {e}")
        except (OSError, httpx.ConnectError, httpx.Timeout, httpx.RemoteProtocolError) as e:
            logger.warning("HubSpot: connection/socket error — %s", e)
            raise RuntimeError(f"HubSpot connection error: {e}")

        location = r.headers.get("location") or r.headers.get("Location")
        if not location:
            return r.json() if r.content else {}
        max_redirects -= 1
        if max_redirects == 0:
            raise ValueError("HubSpot: too many redirects")
        url = location
        try:
            _check_url_ssrf(url)
        except ValueError as e:
            raise RuntimeError(f"HubSpot redirect SSRF check failed: {e}")
        logger.info("HubSpot: following redirect to %s", url)
    raise ValueError("HubSpot: redirect loop exceeded")


def _flatten(obj):
    """Return {id, ...properties} for a HubSpot object dict."""
    if not obj:
        return {}
    out = {"id": obj.get("id"), "created_at": obj.get("createdAt"), "updated_at": obj.get("updatedAt")}
    out.update(obj.get("properties", {}))
    return out


def run(config, inp, context, logger, creds=None, **kwargs):

    token     = ""
    if creds:
        raw = creds.get("access_token", "") or creds.get("token", "")
        if raw:
            try:   token = json.loads(raw).get("access_token", raw)
            except JSONDecodeError: token = raw
    if not token:
        token = _render(config.get("access_token", ""), context, creds)
    if not token:
        raise ValueError("HubSpot: access_token is required (set via credential or access_token field)")

    op          = _render(config.get("operation", "get_contact"), context, creds)
    object_type = _render(config.get("object_type", "contacts"), context, creds)
    logger.info("HubSpot: op=%s object_type=%s", op, object_type)

    # ── get contact / company / deal ──────────────────────────────────────
    if op in ("get_contact", "get_object"):
        obj_id   = _render(config.get("object_id", ""), context, creds)
        props    = _render(config.get("properties", ""), context, creds)
        qs       = f"?properties={props}" if props else ""
        result   = _req("GET", f"/crm/v3/objects/{object_type}/{obj_id}{qs}", token)
        flat     = _flatten(result)
        logger.info("HubSpot: %s retrieved id=%s", op, flat.get("id"))
        return {"object": flat, "id": flat.get("id"), "properties": result.get("properties", {}), "raw": result}

    # ── create contact / company / deal ───────────────────────────────────
    elif op in ("create_contact", "create_object"):
        logger.info("HubSpot: creating %s", object_type)
        props_raw = _render(config.get("properties", "{}"), context, creds)
        try:   props = json.loads(props_raw) if isinstance(props_raw, str) else props_raw
        except JSONDecodeError: raise ValueError(f"HubSpot create: properties must be valid JSON, got: {props_raw!r}")
        result = _req("POST", f"/crm/v3/objects/{object_type}", token, {"properties": props})
        flat   = _flatten(result)
        logger.info("HubSpot: %s created id=%s", op, flat.get("id"))
        return {"object": flat, "id": flat.get("id"), "properties": result.get("properties", {}), "raw": result}

    # ── update contact / company / deal ───────────────────────────────────
    elif op in ("update_contact", "update_object"):
        obj_id    = _render(config.get("object_id", ""), context, creds)
        logger.info("HubSpot: updating %s id=%s", object_type, obj_id)
        props_raw = _render(config.get("properties", "{}"), context, creds)
        try:   props = json.loads(props_raw) if isinstance(props_raw, str) else props_raw
        except JSONDecodeError: raise ValueError(f"HubSpot update: properties must be valid JSON, got: {props_raw!r}")
        result = _req("PATCH", f"/crm/v3/objects/{object_type}/{obj_id}", token, {"properties": props})
        flat   = _flatten(result)
        logger.info("HubSpot: %s updated id=%s", op, flat.get("id"))
        return {"object": flat, "id": flat.get("id"), "properties": result.get("properties", {}), "raw": result}

    # ── search contacts / companies / deals ───────────────────────────────
    elif op == "search":
        logger.info("HubSpot: search %s", object_type)
        filters_raw  = _render(config.get("filters", "[]"), context, creds)
        props_str    = _render(config.get("properties", ""), context, creds)
        try: limit = int(_render(config.get("limit", "20"), context, creds))
        except (ValueError, TypeError): limit = 20
        after_cursor = _render(config.get("after", ""), context, creds)
        try:   filters = json.loads(filters_raw) if isinstance(filters_raw, str) else filters_raw
        except JSONDecodeError: filters = []
        body = {"filterGroups": [{"filters": filters}], "limit": min(limit, 200)}
        if props_str:
            body["properties"] = [p.strip() for p in props_str.split(",") if p.strip()]
        if after_cursor:
            body["after"] = after_cursor
        result  = _req("POST", f"/crm/v3/objects/{object_type}/search", token, body)
        results = [_flatten(r) for r in result.get("results", [])]
        paging  = result.get("paging", {})
        logger.info("HubSpot: search returned %d results has_more=%s", len(results), bool(paging.get("next")))
        return {
            "results":  results,
            "count":    len(results),
            "total":    result.get("total", len(results)),
            "after":    paging.get("next", {}).get("after"),
            "has_more": bool(paging.get("next")),
            "raw":      result,
        }

    # ── get deal ──────────────────────────────────────────────────────────
    elif op == "get_deal":
        deal_id = _render(config.get("deal_id", ""), context, creds)
        result  = _req("GET", f"/crm/v3/objects/deals/{deal_id}?properties=dealname,amount,dealstage,closedate,pipeline", token)
        flat    = _flatten(result)
        logger.info("HubSpot: get_deal retrieved id=%s", flat.get("id"))
        return {"object": flat, "id": flat.get("id"), "properties": result.get("properties", {}), "raw": result}

    # ── create deal ───────────────────────────────────────────────────────
    elif op == "create_deal":
        logger.info("HubSpot: creating deal")
        props_raw = _render(config.get("properties", "{}"), context, creds)
        try:   props = json.loads(props_raw) if isinstance(props_raw, str) else props_raw
        except JSONDecodeError: raise ValueError(f"HubSpot create_deal: properties must be valid JSON, got: {props_raw!r}")
        result = _req("POST", "/crm/v3/objects/deals", token, {"properties": props})
        flat   = _flatten(result)
        logger.info("HubSpot: create_deal created id=%s", flat.get("id"))
        return {"object": flat, "id": flat.get("id"), "properties": result.get("properties", {}), "raw": result}

    # ── associate ─────────────────────────────────────────────────────────
    elif op == "associate":
        from_type = _render(config.get("from_object_type", ""), context, creds)
        from_id   = _render(config.get("from_object_id", ""), context, creds)
        to_type   = _render(config.get("to_object_type", ""), context, creds)
        to_id     = _render(config.get("to_object_id", ""), context, creds)
        assoc_type= _render(config.get("association_type", ""), context, creds)
        body      = [{"associationCategory": assoc_type}]
        _req("PUT", f"/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}", token, body)
        logger.info("HubSpot: associate %s/%s -> %s/%s", from_type, from_id, to_type, to_id)
        return {"ok": True}

    else:
        raise ValueError(f"HubSpot: unknown operation '{op}'")

