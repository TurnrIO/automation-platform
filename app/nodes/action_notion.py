"""Notion API action node."""
import ipaddress
import logging
import socket
import urllib.parse
import json
import httpx
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)
NODE_TYPE = "action.notion"
LABEL = "Notion"

_API_BASE = "https://api.notion.com/v1"

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
    """Validate URL scheme and resolve hostname for SSRF check."""
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"Notion: only http/https URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Notion: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Notion: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _, _, _, sockaddr) in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Notion: URL resolves to blocked address {ip_str}. "
                f"URL: {url[:100]}"
            )


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with Notion API."""

    token = _render(config.get('token', ''), context, creds)
    cred_name = _render(config.get('credential', ''), context, creds)

    if cred_name and creds and not token:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                token = json.loads(raw).get('token', raw)
            except (JSONDecodeError, ValueError):
                token = raw

    if not token:
        raise ValueError("Notion: no integration token configured")

    action = config.get('action', 'query_database')
    database_id = _render(config.get('database_id', ''), context, creds)
    page_id = _render(config.get('page_id', ''), context, creds)
    query = _render(config.get('query', ''), context, creds)

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28',
    }
    base = _API_BASE

    def notion(method, url, **kw):
        # SSRF: validate resolved URL before making request
        try:
            _check_url_ssrf(url)
        except ValueError as exc:
            return {"__error": f"Notion SSRF check failed: {exc}"}
        try:
            r = httpx.request(method, url, headers=headers, timeout=30, **kw)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as exc:
            logger.warning("Notion: HTTP error — %s", exc)
            return {"__error": f"Notion HTTP error: {exc}"}
        except OSError as exc:
            logger.warning("Notion: connection error — %s", exc)
            return {"__error": f"Notion connection error: {exc}"}
        except (AttributeError, KeyError, TypeError) as exc:
            logger.warning("Notion: unexpected error — %s", exc)
            return {"__error": f"Notion request failed: {exc}"}

    logger.info("Notion: action=%s", action)
    if action == 'query_database':
        if not database_id:
            raise ValueError("Notion query_database: database_id required")

        body = {}
        if config.get('filter_json'):
            try:
                body['filter'] = json.loads(_render(config['filter_json'], context, creds))
            except (JSONDecodeError, ValueError):
                pass
        if config.get('sorts_json'):
            try:
                body['sorts'] = json.loads(_render(config['sorts_json'], context, creds))
            except (JSONDecodeError, ValueError):
                pass
        try: page_size = int(_render(str(config.get('page_size', 50)), context, creds))
        except (ValueError, TypeError): page_size = 50
        body['page_size'] = page_size

        logger.info("Notion: query_database id=%s", database_id)
        data = notion('POST', f'{base}/databases/{database_id}/query', json=body)

        # Flatten page properties for easy downstream use
        pages = []
        for p in data.get('results', []):
            props = {}
            for k, v in p.get('properties', {}).items():
                t = v.get('type', '')
                if t == 'title':
                    props[k] = ''.join(x['plain_text'] for x in v.get('title', []))
                elif t == 'rich_text':
                    props[k] = ''.join(x['plain_text'] for x in v.get('rich_text', []))
                elif t == 'number':
                    props[k] = v.get('number')
                elif t == 'select':
                    props[k] = (v.get('select') or {}).get('name')
                elif t == 'multi_select':
                    props[k] = [x['name'] for x in v.get('multi_select', [])]
                elif t == 'checkbox':
                    props[k] = v.get('checkbox')
                elif t == 'date':
                    props[k] = (v.get('date') or {}).get('start')
                elif t == 'url':
                    props[k] = v.get('url')
                elif t == 'email':
                    props[k] = v.get('email')
                elif t == 'phone_number':
                    props[k] = v.get('phone_number')
                else:
                    props[k] = v
            pages.append({'id': p['id'], 'url': p.get('url'), 'properties': props})

        return {'pages': pages, 'count': len(pages), 'has_more': data.get('has_more', False)}

    elif action == 'get_page':
        logger.info("Notion: get_page id=%s", page_id)
        if not page_id:
            raise ValueError("Notion get_page: page_id required")
        return notion('GET', f'{base}/pages/{page_id}')

    elif action == 'create_page':
        logger.info("Notion: create_page in db=%s", database_id)
        if not database_id:
            raise ValueError("Notion create_page: database_id required")

        props_raw = _render(config.get('properties_json', '{}'), context, creds)
        try:
            props = json.loads(props_raw)
        except (JSONDecodeError, ValueError):
            raise ValueError("Notion create_page: properties_json must be valid JSON")

        # Auto-wrap plain string values as title/rich_text
        wrapped = {}
        for k, v in props.items():
            if isinstance(v, str):
                wrapped[k] = {'rich_text': [{'type': 'text', 'text': {'content': v}}]}
            else:
                wrapped[k] = v

        body = {'parent': {'database_id': database_id}, 'properties': wrapped}

        # Title field special-casing
        title_val = _render(config.get('title', ''), context, creds)
        title_key = config.get('title_field', 'Name')
        if title_val:
            body['properties'][title_key] = {'title': [{'type': 'text', 'text': {'content': title_val}}]}

        return notion('POST', f'{base}/pages', json=body)

    elif action == 'update_page':
        logger.info("Notion: update_page id=%s", page_id)
        if not page_id:
            raise ValueError("Notion update_page: page_id required")

        props_raw = _render(config.get('properties_json', '{}'), context, creds)
        try:
            props = json.loads(props_raw)
        except (JSONDecodeError, ValueError):
            raise ValueError("Notion update_page: properties_json must be valid JSON")

        return notion('PATCH', f'{base}/pages/{page_id}', json={'properties': props})

    elif action == 'search':
        logger.info("Notion: search query=%r", query[:50] if query else '')
        try: page_size = int(_render(str(config.get('page_size', 20)), context, creds))
        except (ValueError, TypeError): page_size = 20
        body = {'page_size': page_size}
        if query:
            body['query'] = query
        return notion('POST', f'{base}/search', json=body)

    elif action == 'append_blocks':
        logger.info("Notion: append_blocks page_id=%s", page_id)
        if not page_id:
            raise ValueError("Notion append_blocks: page_id required")

        content = _render(config.get('content', ''), context, creds)

        # Treat content as plain paragraph text
        blocks = [{'object': 'block', 'type': 'paragraph',
                   'paragraph': {'rich_text': [{'type': 'text', 'text': {'content': content}}]}}]

        return notion('PATCH', f'{base}/blocks/{page_id}/children', json={'children': blocks})

    else:
        raise ValueError(f"Notion: unknown action '{action}'")
