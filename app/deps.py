"""Shared FastAPI dependencies — auth guards used across all routers."""
import logging
from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse

from app.core.db import (
    users_exist, get_api_token_by_hash, touch_api_token,
    get_flow_permission, FLOW_ROLE_LEVELS,
    get_workspace, get_workspace_member,
    list_user_workspaces, get_default_workspace,
)

ROLE_LEVELS = {"viewer": 0, "admin": 1, "owner": 2}

# API token scopes in ascending permission order.
# read   — GET endpoints only (no state mutations)
# run    — read + trigger runs / replay / cancel
# manage — full API access (equivalent to the old token behaviour)
TOKEN_SCOPE_LEVELS = {"read": 0, "run": 1, "manage": 2}

log = logging.getLogger(__name__)


def _extract_raw_token(request: Request) -> str:
    """Extract the raw API token from the request, in preference order.

    Preferred (safe): Authorization: Bearer <token> header or legacy x-api-token /
    x-admin-token headers.  Deprecated (leaks into server logs): ?token= query
    parameter — still accepted for backwards-compatibility but emits a warning so
    callers can migrate.
    """
    # 1. Standard Bearer header — recommended for all API clients
    auth_header = request.headers.get("Authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    # 2. Legacy custom headers — safe (not exposed in URLs or logs)
    raw = request.headers.get("x-api-token") or request.headers.get("x-admin-token")
    if raw:
        return raw

    # 3. Query parameter — DEPRECATED: token appears in server logs and browser history
    raw = request.query_params.get("token", "")
    if raw:
        log.warning(
            "API token supplied via ?token= query parameter (deprecated). "
            "Switch to 'Authorization: Bearer <token>'. Path: %s",
            request.url.path,
        )
        return raw

    return ""


def _check_admin(request: Request):
    """Accept session cookie (browser) OR token via Authorization: Bearer / legacy headers."""
    from app.auth import get_current_user, hash_token
    user = get_current_user(request)
    if user:
        return user
    raw = _extract_raw_token(request)
    if raw:
        th = hash_token(raw)
        tok = get_api_token_by_hash(th)
        if tok:
            touch_api_token(th)
            scope = tok.get("scope", "manage")
            return {
                "id": 0,
                "username": f"api:{tok['name']}",
                "role": "owner",
                "token_scope": scope,
            }
    raise HTTPException(401, "Authentication required")


# Sentinel to detect whether a Request or user dict was passed.
_UNSET = object()


def _require_scope(user_or_request, min_scope: str, *, _user=_UNSET, **kwargs):
    """Require at least *min_scope* when authenticated via an API token.

    Supports two call signatures:
      _require_scope(request, min_scope)  — normal runtime call
      _require_scope(user_dict, min_scope, _user=user_dict)  — test call
    """
    if _user is not _UNSET:
        user = _user
    elif isinstance(user_or_request, dict):
        # pre-resolved user dict passed directly (test path)
        user = user_or_request
    else:
        user = _check_admin(user_or_request)
    scope = user.get("token_scope")
    if scope is not None:
        # token-based auth — enforce scope
        if TOKEN_SCOPE_LEVELS.get(scope, 0) < TOKEN_SCOPE_LEVELS.get(min_scope, 0):
            raise HTTPException(
                403,
                f"This action requires token scope '{min_scope}' "
                f"(your token has scope '{scope}')",
            )
    return user


def _require_run_scope(request: Request):
    """Token must have at least 'run' scope (session users always pass)."""
    return _require_scope(request, "run")


def _require_manage_scope(request: Request):
    """Token must have 'manage' scope (session users always pass)."""
    return _require_scope(request, "manage")


def _require_writer(user_or_request, *, _user=_UNSET, **kwargs):
    """Authenticated + admin or owner role (viewers are read-only).

    Supports both call signatures (see _require_scope).
    """
    if _user is not _UNSET:
        user = _user
    elif isinstance(user_or_request, dict):
        user = user_or_request
    else:
        user = _check_admin(user_or_request)
    if ROLE_LEVELS.get(user.get("role", "viewer"), 0) < 1:
        raise HTTPException(403, "This action requires admin or owner role")
    return user


def _require_owner(user_or_request, *, _user=_UNSET, **kwargs):
    """Authenticated + owner role only.

    Supports both call signatures (see _require_scope).
    """
    if _user is not _UNSET:
        user = _user
    elif isinstance(user_or_request, dict):
        user = user_or_request
    else:
        user = _check_admin(user_or_request)
    if user.get("role") != "owner":
        raise HTTPException(403, "This action requires owner role")
    return user


def _require_admin(user_or_request, *, _user=_UNSET, **kwargs):
    """Authenticated + admin or owner role.

    Supports both call signatures (see _require_scope).
    """
    if _user is not _UNSET:
        user = _user
    elif isinstance(user_or_request, dict):
        user = user_or_request
    else:
        user = _check_admin(user_or_request)
    if ROLE_LEVELS.get(user.get("role", "viewer"), 0) < 1:
        raise HTTPException(403, "This action requires admin or owner role")
    return user

def _check_flow_access(user_or_request, graph_id: int, required_role: str = "viewer", *, _user=None):
    """Enforce per-flow access control for viewer-role users.

    - admin / owner: always granted (skip per-flow check).
    - API token users: always granted (token-level scope already enforced).
    - viewer (global role): must have an explicit flow_permissions row with
      a role >= required_role.

    Supports both call signatures:
      _check_flow_access(request, graph_id, required_role)  — runtime
      _check_flow_access(user_dict, graph_id, required_role)  — test path

    Returns the user dict on success; raises HTTP 403 otherwise.

    required_role must be one of: 'viewer', 'runner', 'editor'.
    """
    if _user is not None:
        user = _user
    elif isinstance(user_or_request, dict):
        user = user_or_request
    else:
        user = _check_admin(user_or_request)
    global_role = user.get("role", "viewer")

    # Admins, owners, and token-authenticated callers bypass per-flow checks.
    if global_role in ("admin", "owner") or user.get("id") == 0:
        return user

    # Viewer global role: check the flow_permissions table.
    fp = get_flow_permission(user["id"], graph_id)
    if fp is None:
        raise HTTPException(403, "You do not have access to this flow")

    granted_level  = FLOW_ROLE_LEVELS.get(fp["role"], 0)
    required_level = FLOW_ROLE_LEVELS.get(required_role, 0)
    if granted_level < required_level:
        raise HTTPException(
            403,
            f"This action requires '{required_role}' access to this flow "
            f"(you have '{fp['role']}')",
        )
    return user


WORKSPACE_COOKIE = "hr_workspace"


def _resolve_workspace(request: Request, user: dict) -> int | None:
    """Determine the active workspace_id for this request.

    Resolution order (first match wins):
    1. X-Workspace-Id header  — explicit per-request override (API clients)
    2. hr_workspace cookie    — last workspace the browser user switched to
    3. User's first workspace  — from workspace_members
    4. Global default          — the workspace with slug='default'
    5. None                    — workspaces table not yet populated (pre-migration)

    The returned workspace_id is validated: the user must be a member (unless
    they are the global owner, who can access any workspace).
    """
    uid = user.get("id", 0)
    is_global_owner = user.get("role") == "owner"

    def _validate(ws_id: int) -> int | None:
        """Return ws_id if valid and the user has access, else None."""
        ws = get_workspace(ws_id)
        if not ws:
            return None
        if is_global_owner or uid == 0:
            return ws_id
        member = get_workspace_member(ws_id, uid)
        return ws_id if member else None

    # 1. Explicit header
    header_val = request.headers.get("X-Workspace-Id", "").strip()
    if header_val:
        try:
            wid = int(header_val)
            result = _validate(wid)
            if result:
                return result
        except (ValueError, TypeError) as exc:
            log.warning("Could not resolve workspace from X-Workspace-Id header: %s", exc)
        except (OSError, RuntimeError) as exc:
            log.warning("Unexpected error resolving workspace from X-Workspace-Id header: %s", exc)

    # 2. Browser cookie
    cookie_val = request.cookies.get(WORKSPACE_COOKIE, "").strip()
    if cookie_val:
        try:
            wid = int(cookie_val)
            result = _validate(wid)
            if result:
                return result
        except (ValueError, TypeError) as exc:
            log.warning("Could not resolve workspace from cookie: %s", exc)
        except (OSError, RuntimeError) as exc:
            log.warning("Unexpected error resolving workspace from cookie: %s", exc)

    # 3. User's first workspace
    if uid and uid != 0:
        try:
            memberships = list_user_workspaces(uid)
            if memberships:
                return memberships[0]["id"]
        except (AttributeError, TypeError, KeyError, OSError) as exc:
            log.warning("Could not resolve workspace from user's workspaces: %s", exc)
        except (OSError, RuntimeError) as exc:
            log.warning("Unexpected error resolving workspace from user's workspaces: %s", exc)


    # 4. Global default workspace
    try:
        default = get_default_workspace()
        if default:
            return default["id"]
    except (AttributeError, TypeError, KeyError, OSError) as exc:
        log.warning("Could not resolve workspace from default workspace: %s", exc)
    except (OSError, RuntimeError) as exc:
        log.warning("Unexpected error resolving workspace from default workspace: %s", exc)

    return None


def _auth_redirect(request: Request):
    """Returns a redirect to /setup or /login if the browser is not authenticated."""
    if not users_exist():
        return RedirectResponse("/setup", status_code=302)
    from app.auth import get_current_user
    if not get_current_user(request):
        next_path = request.url.path
        if request.url.query:
            next_path += "?" + request.url.query
        return RedirectResponse(f"/login?next={next_path}", status_code=302)
    return None
