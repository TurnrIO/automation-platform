"""OAuth2 credential flows for GitHub, Google (Sheets/Drive), and Notion.

Flow:
  1. GET /api/oauth/{provider}/start?cred_name=<name>
       → validates admin session, stores state in Redis, redirects to provider
  2. Provider redirects to GET /api/oauth/{provider}/callback?code=...&state=...
       → exchanges code for token, saves as credential, redirects to /admin

Env vars required per provider (add to .env):
  GitHub : GITHUB_CLIENT_ID  GITHUB_CLIENT_SECRET
  Google : GOOGLE_CLIENT_ID  GOOGLE_CLIENT_SECRET
  Notion : NOTION_CLIENT_ID  NOTION_CLIENT_SECRET
"""
import base64
from json import JSONDecodeError
import json
import logging
import os
import secrets
import time
from urllib.parse import urlencode

import httpx
import redis as redis_lib
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from app.core.db import upsert_credential
from app.deps import _check_admin, _resolve_workspace

log = logging.getLogger(__name__)
router = APIRouter()

# ── Provider definitions ──────────────────────────────────────────────────────

PROVIDERS: dict[str, dict] = {
    "github": {
        "label": "GitHub",
        "auth_url": "https://github.com/login/oauth/authorize",
        "token_url": "https://github.com/login/oauth/access_token",
        "scope": "repo read:user",
        "client_id_env": "GITHUB_CLIENT_ID",
        "client_secret_env": "GITHUB_CLIENT_SECRET",
        "cred_type": "github_oauth",
        "default_cred_name": "github",
    },
    "google": {
        "label": "Google (Sheets / Drive)",
        "auth_url": "https://accounts.google.com/o/oauth2/v2/auth",
        "token_url": "https://oauth2.googleapis.com/token",
        "scope": (
            "https://www.googleapis.com/auth/spreadsheets "
            "https://www.googleapis.com/auth/drive.file"
        ),
        "client_id_env": "GOOGLE_CLIENT_ID",
        "client_secret_env": "GOOGLE_CLIENT_SECRET",
        "cred_type": "google_oauth",
        "default_cred_name": "google-sheets",
        "extra_params": {"access_type": "offline", "prompt": "consent"},
    },
    "notion": {
        "label": "Notion",
        "auth_url": "https://api.notion.com/v1/oauth/authorize",
        "token_url": "https://api.notion.com/v1/oauth/token",
        "scope": None,  # Notion does not use a scope parameter
        "client_id_env": "NOTION_CLIENT_ID",
        "client_secret_env": "NOTION_CLIENT_SECRET",
        "cred_type": "notion_oauth",
        "default_cred_name": "notion",
    },
}

STATE_TTL = 300  # seconds — OAuth state lives 5 minutes in Redis


# ── Helpers ───────────────────────────────────────────────────────────────────

def _redis() -> redis_lib.Redis:
    return redis_lib.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))


def _redirect_uri(provider: str) -> str:
    base = os.environ.get("APP_URL", "http://localhost").rstrip("/")
    return f"{base}/api/oauth/{provider}/callback"


def _admin_url(path: str = "") -> str:
    base = os.environ.get("APP_URL", "http://localhost").rstrip("/")
    return f"{base}/admin{path}"


def _exchange_github(cfg: dict, code: str, provider: str) -> dict:
    resp = httpx.post(
        cfg["token_url"],
        data={
            "client_id": os.environ.get(cfg["client_id_env"], ""),
            "client_secret": os.environ.get(cfg["client_secret_env"], ""),
            "code": code,
            "redirect_uri": _redirect_uri(provider),
        },
        headers={"Accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _exchange_google(cfg: dict, code: str, provider: str) -> dict:
    resp = httpx.post(
        cfg["token_url"],
        data={
            "client_id": os.environ.get(cfg["client_id_env"], ""),
            "client_secret": os.environ.get(cfg["client_secret_env"], ""),
            "code": code,
            "redirect_uri": _redirect_uri(provider),
            "grant_type": "authorization_code",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _exchange_notion(cfg: dict, code: str, provider: str) -> dict:
    client_id = os.environ.get(cfg["client_id_env"], "")
    client_secret = os.environ.get(cfg["client_secret_env"], "")
    creds_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = httpx.post(
        cfg["token_url"],
        json={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": _redirect_uri(provider),
        },
        headers={
            "Authorization": f"Basic {creds_b64}",
            "Content-Type": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _build_credential_secret(provider: str, token_data: dict) -> str:
    """Convert provider token response into the JSON stored in the credentials table."""
    if provider == "github":
        return json.dumps({
            "token": token_data.get("access_token", ""),
            "provider": "github",
        })
    if provider == "google":
        return json.dumps({
            "access_token": token_data.get("access_token", ""),
            "refresh_token": token_data.get("refresh_token", ""),
            "token_expiry": int(time.time()) + int(token_data.get("expires_in", 3600)),
            "provider": "google",
        })
    if provider == "notion":
        return json.dumps({
            "token": token_data.get("access_token", ""),
            "workspace_name": token_data.get("workspace_name", ""),
            "workspace_icon": token_data.get("workspace_icon", ""),
            "provider": "notion",
        })
    raise ValueError(f"Unknown provider: {provider}")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/api/oauth/providers")
def api_oauth_providers(request: Request):
    """Return which OAuth providers are configured (client_id set)."""
    _check_admin(request)
    return {
        p: bool(os.environ.get(cfg["client_id_env"], "").strip())
        for p, cfg in PROVIDERS.items()
    }


@router.get("/api/oauth/{provider}/start")
def oauth_start(provider: str, cred_name: str, request: Request):
    """Kick off the OAuth authorization code flow for the given provider."""
    if provider not in PROVIDERS:
        return RedirectResponse(_admin_url("?oauth_error=unknown_provider"))

    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    cfg = PROVIDERS[provider]

    client_id = os.environ.get(cfg["client_id_env"], "").strip()
    if not client_id:
        return RedirectResponse(_admin_url(f"?oauth_error={cfg['client_id_env']}_not_set"))

    # Sanitise credential name — same rules as the UI
    safe_name = "".join(c for c in cred_name.strip() if c.isalnum() or c in "-_") or cfg["default_cred_name"]

    state = secrets.token_urlsafe(32)
    try:
        r = _redis()
        r.setex(
            f"oauth:state:{state}",
            STATE_TTL,
            json.dumps({
                "provider": provider,
                "cred_name": safe_name,
                "workspace_id": workspace_id,
                "user_id": user["id"],
            }),
        )
    except (AttributeError, TypeError, OSError) as exc:
        log.error("oauth_start: Redis error: %s", exc)
        return RedirectResponse(_admin_url("?oauth_error=redis_unavailable"))

    params: dict = {
        "client_id": client_id,
        "redirect_uri": _redirect_uri(provider),
        "state": state,
        "response_type": "code",
    }
    if cfg.get("scope"):
        params["scope"] = cfg["scope"]
    if cfg.get("extra_params"):
        params.update(cfg["extra_params"])

    auth_url = cfg["auth_url"] + "?" + urlencode(params)
    return RedirectResponse(auth_url)


@router.get("/api/oauth/{provider}/callback")
def oauth_callback(
    provider: str,
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
):
    """Handle the provider redirect, exchange the code, and save a credential."""
    if error or not code or not state:
        return RedirectResponse(_admin_url(f"?oauth_error={error or 'cancelled'}"))

    if provider not in PROVIDERS:
        return RedirectResponse(_admin_url("?oauth_error=unknown_provider"))

    # Validate & consume state
    try:
        r = _redis()
        raw = r.get(f"oauth:state:{state}")
    except (AttributeError, TypeError, OSError) as exc:
        log.error("oauth_callback: Redis error: %s", exc)
        return RedirectResponse(_admin_url("?oauth_error=redis_unavailable"))

    if not raw:
        return RedirectResponse(_admin_url("?oauth_error=state_expired_or_invalid"))

    try:
        ctx = json.loads(raw)
    except JSONDecodeError:
        ctx = {}

    if ctx.get("provider") != provider:
        return RedirectResponse(_admin_url("?oauth_error=state_mismatch"))

    cfg = PROVIDERS[provider]

    # Exchange authorisation code for access token
    try:
        if provider == "github":
            token_data = _exchange_github(cfg, code, provider)
        elif provider == "google":
            token_data = _exchange_google(cfg, code, provider)
        elif provider == "notion":
            token_data = _exchange_notion(cfg, code, provider)
        else:
            return RedirectResponse(_admin_url("?oauth_error=unsupported_provider"))
    except (httpx.HTTPStatusError, httpx.RequestError, OSError) as exc:
        log.error("oauth_callback: token exchange failed for %s: %s", provider, exc)
        return RedirectResponse(_admin_url("?oauth_error=token_exchange_failed"))

    # Persist as a credential — only delete state token after this succeeds
    try:
        secret = _build_credential_secret(provider, token_data)
        cred_name = ctx["cred_name"]
        workspace_id = ctx.get("workspace_id")
        note = f"Connected via OAuth on {time.strftime('%Y-%m-%d')}"
        upsert_credential(cred_name, cfg["cred_type"], secret, note, workspace_id=workspace_id)
        # State consumed — safe to delete now
        r.delete(f"oauth:state:{state}")
    except (AttributeError, TypeError, RuntimeError, ValueError) as exc:
        log.error("oauth_callback: failed to save credential: %s", exc)
        return RedirectResponse(_admin_url("?oauth_error=save_failed"))

    log.info("oauth_callback: saved %s credential %r for workspace %s", provider, cred_name, workspace_id)
    return RedirectResponse(_admin_url(f"?oauth_success={provider}&cred={cred_name}"))
