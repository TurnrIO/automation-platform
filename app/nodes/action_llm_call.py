"""LLM call action node.

Calls an OpenAI-compatible LLM API.

Config fields:
  model     — model name, e.g. gpt-4o-mini (default: gpt-4o-mini)
  prompt    — user prompt (supports {{template}} rendering)
  system    — system prompt (default: You are a helpful assistant.)
  api_key   — API key; falls back to OPENAI_API_KEY env var
  api_base  — base URL for the API, e.g. https://api.openai.com/v1
              ⚠️  SSRF protection: only HTTPS URLs allowed; hostname is
                  resolved via DNS and checked against blocked IP ranges.

All config fields support {{template}} rendering.
"""
import logging

logger = logging.getLogger(__name__)
import socket
import ipaddress
import urllib.parse
import os
from app.nodes._utils import _render

NODE_TYPE = "action.llm_call"
LABEL     = "LLM Call"

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


def _check_ssrf(url: str) -> None:
    """Validate URL scheme and resolve hostname for SSRF check.
    Raises ValueError if URL is unsafe.
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise ValueError(
            f"LLM Call: only https:// api_base URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"LLM Call: api_base has no valid hostname: {url[:100]}")
    try:
        infos = socket.getaddrinfo(host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError(f"LLM Call: could not resolve hostname '{host}' in api_base: {url[:100]}")
    for (family, _, _, _, sockaddr) in infos:
        if family in (socket.AF_INET, socket.AF_INET6):
            ip_str = sockaddr[0]
            if _blocked_ip(ip_str):
                raise ValueError(
                    f"LLM Call: api_base resolves to blocked IP {ip_str}. "
                    f"Hostname '{host}' is not allowed. URL: {url[:100]}"
                )


def run(config, inp, context, logger, creds=None, **kwargs):
    """Call OpenAI-compatible LLM API."""
    import httpx

    logger.info("LLM Call: starting")

    model    = _render(config.get('model', 'gpt-4o-mini'), context, creds)
    prompt   = _render(config.get('prompt', ''), context, creds)
    system   = _render(config.get('system', 'You are a helpful assistant.'), context, creds)
    api_key  = _render(config.get('api_key', ''), context, creds) or os.environ.get('OPENAI_API_KEY', '')
    api_base = _render(config.get('api_base', ''), context, creds) or 'https://api.openai.com/v1'

    if not api_key:
        raise ValueError("LLM Call: no api_key configured and OPENAI_API_KEY env not set")

    # ── SSRF check on api_base ───────────────────────────────────────────
    try:
        _check_ssrf(api_base)
    except ValueError as exc:
        logger.warning("LLM Call: SSRF check failed — %s", exc)
        return {"__error": f"SSRF check failed: {exc}", "model": model}

    try:
        resp = httpx.post(
            f"{api_base.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt}
                ]
            },
            timeout=60
        )
        resp.raise_for_status()
    except (httpx.HTTPError, OSError) as exc:
        logger.warning("LLM Call: HTTP error calling %s — %s", api_base, exc)
        return {"__error": f"LLM call failed: HTTP error — {exc}", "model": model}

    data = resp.json()
    try:
        choices = data['choices']
        if not isinstance(choices, list) or not choices:
            raise ValueError("LLM Call: API returned empty or non-list 'choices' field")
        reply = choices[0]['message']['content']
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        logger.warning("LLM Call: unexpected API response shape — %s", exc)
        return {"__error": f"LLM call failed: API response shape error — {exc}", "model": model}
    tokens = data.get('usage', {}).get('total_tokens', 0)

    return {'response': reply, 'model': model, 'tokens': tokens}
