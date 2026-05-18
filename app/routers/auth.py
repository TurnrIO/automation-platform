"""Auth, user management, and API token routers."""
import logging
import redis
import secrets as _sec
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from app.deps import _check_admin, _require_writer, _require_owner, _resolve_workspace

log = logging.getLogger(__name__)

# ── Login brute-force protection ──────────────────────────────────────────────
_MAX_ATTEMPTS  = 5      # failed attempts before lockout
_LOCKOUT_S     = 900    # lockout duration in seconds (15 minutes)
_ATTEMPT_TTL_S = 3600   # sliding window for attempt counter (1 hour)


def _login_redis():
    """Return a Redis client, or None if Redis is unavailable."""
    try:
        import os
        url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
        r = redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
        r.ping()
        return r
    except (OSError, RuntimeError, AttributeError, redis.exceptions.RedisError):
        # OSError/RuntimeError/AttributeError cover the original targets.
        # redis.exceptions.RedisError catches ConnectionError/TimeoutError/etc.
        # (NOT subclasses of OSError — they extend RedisError).
        # In test/CI environments without Redis, this is expected; return None.
        return None


def _check_login_allowed(ip: str) -> None:
    """Raise HTTP 429 if this IP is currently locked out.

    Fail-closed: if Redis is unavailable, reject logins rather than
    bypass brute-force protection.
    """
    r = _login_redis()
    if r is None:
        log.critical("Login check unavailable — Redis is down; rejecting login attempt from %s", ip)
        raise HTTPException(503, "Login service temporarily unavailable")
    lockout_key = f"hiverunr:login:lockout:{ip}"
    if r.exists(lockout_key):
        raise HTTPException(
            429,
            "Too many failed login attempts. Please try again later.",
        )


def _record_login_failure(ip: str) -> None:
    """Increment the failure counter; lock out the IP after _MAX_ATTEMPTS.

    Fail-closed: if Redis is unavailable, log and raise rather than silently permit.
    """
    r = _login_redis()
    if r is None:
        log.critical("Cannot record login failure — Redis is down for IP %s", ip)
        raise HTTPException(503, "Login service temporarily unavailable")
    attempt_key = f"hiverunr:login:attempts:{ip}"
    lockout_key = f"hiverunr:login:lockout:{ip}"
    count = r.incr(attempt_key)
    r.expire(attempt_key, _ATTEMPT_TTL_S)
    if count >= _MAX_ATTEMPTS:
        r.setex(lockout_key, _LOCKOUT_S, "1")
        r.delete(attempt_key)
        log.warning("Login lockout triggered for IP %s after %d failed attempts", ip, count)


def _clear_login_failures(ip: str) -> None:
    """Clear failure counters on successful login."""
    r = _login_redis()
    if r is None:
        return
    r.delete(f"hiverunr:login:attempts:{ip}")
    r.delete(f"hiverunr:login:lockout:{ip}")
from app.core.db import (
    users_exist, create_user, get_user_by_username, get_user_by_id, list_users,
    update_user_password, update_user_role, delete_user,
    create_session, delete_session_by_token_hash,
    create_api_token, list_api_tokens, get_api_token_by_hash, touch_api_token, delete_api_token,
    get_owner_user, create_password_reset_token, get_password_reset_by_token,
    consume_password_reset_token,
    log_audit,
    get_invite_by_hash, consume_invite_token, set_flow_permission, get_user_by_email,
)

router = APIRouter()


def _user_in_workspace(user_id: int, workspace_id: int) -> bool:
    """Return True if user_id is a member of workspace_id."""
    from app.core.db import get_workspace_member
    return get_workspace_member(workspace_id, user_id) is not None


# ── Caddy forward_auth gate ───────────────────────────────────────────────────
@router.get("/api/auth/check", include_in_schema=False)
def auth_check(request: Request):
    """Called by Caddy forward_auth before proxying to Flower.

    Returns 200 if authenticated, 401 if not.
    """
    from app.auth import get_current_user, hash_token
    if get_current_user(request):
        return Response(status_code=200)
    raw = (request.headers.get("x-api-token")
           or request.headers.get("x-admin-token"))
    if raw:
        th = hash_token(raw)
        tok = get_api_token_by_hash(th)
        if tok:
            touch_api_token(th)
            return Response(status_code=200)
    return Response(status_code=401)


# ── Auth endpoints ────────────────────────────────────────────────────────────
@router.get("/api/auth/status")
def auth_status(request: Request):
    from app.auth import get_current_user
    from app.crypto import encryption_configured
    setup_needed = not users_exist()
    user = get_current_user(request)
    return {
        "setup_needed": setup_needed,
        "logged_in": user is not None,
        "encryption_configured": encryption_configured(),
        "user": {"id": user["id"], "username": user["username"],
                 "email": user["email"], "role": user["role"]} if user else None,
    }


class SetupBody(BaseModel):
    username: str
    email: str
    password: str


@router.post("/api/auth/setup")
def auth_setup(body: SetupBody):
    from app.auth import hash_password, hash_token, generate_token, SESSION_COOKIE, SESSION_DAYS
    if users_exist():
        raise HTTPException(400, "Setup already completed — an owner account already exists")
    if len(body.password) < 8:
        raise HTTPException(422, "Password must be at least 8 characters")
    user = create_user(body.username.strip(), body.email.strip().lower(),
                       hash_password(body.password), "owner")
    token = generate_token()
    create_session(user["id"], hash_token(token))
    resp = JSONResponse({"ok": True, "user": {"id": user["id"],
                         "username": user["username"], "role": user["role"]}})
    resp.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax",
                    max_age=SESSION_DAYS * 86400)
    return resp


class LoginBody(BaseModel):
    username: str
    password: str


@router.post("/api/auth/login")
def auth_login(body: LoginBody, request: Request):
    from app.auth import verify_password, hash_token, generate_token, SESSION_COOKIE, SESSION_DAYS
    # Prefer X-Forwarded-For (set by Caddy/nginx) so we rate-limit the real client
    ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
          or request.client.host
          or "unknown")
    _check_login_allowed(ip)
    user = get_user_by_username(body.username.strip())
    if not user or not verify_password(body.password, user["password_hash"]):
        _record_login_failure(ip)
        raise HTTPException(401, "Invalid username or password")
    _clear_login_failures(ip)
    token = generate_token()
    create_session(user["id"], hash_token(token))
    resp = JSONResponse({"ok": True, "user": {"id": user["id"],
                         "username": user["username"], "role": user["role"]}})
    resp.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax",
                    max_age=SESSION_DAYS * 86400)
    return resp


@router.post("/api/auth/logout")
def auth_logout(request: Request):
    from app.auth import SESSION_COOKIE, hash_token
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        delete_session_by_token_hash(hash_token(token))
    resp = JSONResponse({"ok": True})
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@router.get("/api/auth/me")
def auth_me(request: Request):
    user = _check_admin(request)
    return {"id": user["id"], "username": user["username"],
            "email": user.get("email", ""), "role": user["role"]}


class ChangePasswordBody(BaseModel):
    current_password: str
    new_password: str


@router.post("/api/auth/change-password")
def auth_change_password(body: ChangePasswordBody, request: Request):
    from app.auth import verify_password, hash_password
    user = _check_admin(request)
    if user["id"] == 0:
        raise HTTPException(400, "Token-based admin cannot change password here")
    db_user = get_user_by_id(user["id"])
    if not verify_password(body.current_password, db_user["password_hash"]):
        raise HTTPException(401, "Current password is incorrect")
    if len(body.new_password) < 8:
        raise HTTPException(422, "New password must be at least 8 characters")
    update_user_password(user["id"], hash_password(body.new_password))
    return {"ok": True}


# ── User management ───────────────────────────────────────────────────────────
@router.get("/api/users")
def api_list_users(request: Request):
    _require_writer(request)
    return list_users()


class CreateUserBody(BaseModel):
    username: str
    email: str
    password: str
    role: str = "viewer"


@router.post("/api/users")
def api_create_user(body: CreateUserBody, request: Request):
    from app.auth import hash_password
    actor = _require_writer(request)
    if body.role == "owner":
        raise HTTPException(400, "Cannot create another owner account")
    if body.role == "admin" and actor.get("role") != "owner":
        raise HTTPException(403, "Only owner can create admin accounts")
    if body.role not in ("viewer", "admin"):
        raise HTTPException(422, f"Invalid role '{body.role}'")
    if len(body.password) < 8:
        raise HTTPException(422, "Password must be at least 8 characters")
    try:
        user = create_user(body.username.strip(), body.email.strip().lower(),
                           hash_password(body.password), body.role)
    except (ValueError, RuntimeError, TypeError) as e:
        raise HTTPException(400, str(e))
    log_audit(actor["username"], "user.create", "user", user["id"],
              {"username": user["username"], "role": user["role"]},
              request.client.host if request.client else None)
    return {"id": user["id"], "username": user["username"],
            "email": user["email"], "role": user["role"]}


class UpdateRoleBody(BaseModel):
    role: str


@router.patch("/api/users/{user_id}/role")
def api_update_user_role(user_id: int, body: UpdateRoleBody, request: Request):
    actor = _require_owner(request)
    workspace_id = _resolve_workspace(request, actor)
    target = get_user_by_id(user_id)
    if not target:
        raise HTTPException(404, "User not found")
    # enforce workspace ownership when caller has a workspace context
    if workspace_id is not None and not _user_in_workspace(user_id, workspace_id):
        raise HTTPException(403, "Cross-workspace user access denied")
    if target["role"] == "owner":
        raise HTTPException(400, "Cannot change the owner's role")
    if body.role == "owner":
        raise HTTPException(400, "Cannot promote to owner")
    if body.role not in ("viewer", "admin"):
        raise HTTPException(422, f"Invalid role '{body.role}'")
    update_user_role(user_id, body.role)
    log_audit(actor["username"], "user.update_role", "user", user_id,
              {"username": target["username"], "old_role": target["role"], "new_role": body.role},
              request.client.host if request.client else None)
    return {"ok": True}


class ResetPasswordBody(BaseModel):
    new_password: str


@router.post("/api/users/{user_id}/reset-password")
def api_reset_user_password(user_id: int, body: ResetPasswordBody, request: Request):
    from app.auth import hash_password
    actor = _require_writer(request)
    workspace_id = _resolve_workspace(request, actor)
    target = get_user_by_id(user_id)
    if not target:
        raise HTTPException(404, "User not found")
    # enforce workspace ownership when caller has a workspace context
    if workspace_id is not None and not _user_in_workspace(user_id, workspace_id):
        raise HTTPException(403, "Cross-workspace user access denied")
    if target["role"] == "owner" and actor.get("role") != "owner":
        raise HTTPException(403, "Only owner can reset the owner's password")
    if len(body.new_password) < 8:
        raise HTTPException(422, "Password must be at least 8 characters")
    update_user_password(user_id, hash_password(body.new_password))
    log_audit(actor["username"], "user.reset_password", "user", user_id,
              {"username": target["username"]},
              request.client.host if request.client else None)
    return {"ok": True}


@router.delete("/api/users/{user_id}")
def api_delete_user(user_id: int, request: Request):
    actor = _require_writer(request)
    workspace_id = _resolve_workspace(request, actor)
    target = get_user_by_id(user_id)
    if not target:
        raise HTTPException(404, "User not found")
    if target["role"] == "owner":
        raise HTTPException(400, "Cannot delete the owner account")
    if actor.get("id") == user_id:
        raise HTTPException(400, "Cannot delete your own account")
    # enforce workspace ownership when caller has a workspace context
    if workspace_id is not None and not _user_in_workspace(user_id, workspace_id):
        raise HTTPException(403, "Cross-workspace user access denied")
    delete_user(user_id)
    log_audit(actor["username"], "user.delete", "user", user_id,
              {"username": target["username"], "role": target["role"]},
              request.client.host if request.client else None)
    return {"ok": True}


# ── API token management ──────────────────────────────────────────────────────
@router.get("/api/tokens")
def api_list_tokens(request: Request):
    user = _require_owner(request)
    workspace_id = _resolve_workspace(request, user)
    return list_api_tokens(workspace_id=workspace_id)


class CreateTokenBody(BaseModel):
    name: str
    scope: str = "manage"
    expires_days: int | None = None   # None = never expires


@router.post("/api/tokens")
def api_create_token(body: CreateTokenBody, request: Request):
    from app.auth import hash_token
    from app.core.db import API_TOKEN_SCOPES
    import datetime as _dt
    actor = _require_owner(request)
    if not body.name.strip():
        raise HTTPException(422, "Token name is required")
    if body.scope not in API_TOKEN_SCOPES:
        raise HTTPException(422, f"scope must be one of {list(API_TOKEN_SCOPES)}")
    expires_at = None
    if body.expires_days is not None:
        if body.expires_days < 1:
            raise HTTPException(422, "expires_days must be at least 1")
        expires_at = _dt.datetime.utcnow() + _dt.timedelta(days=body.expires_days)
    raw = "hr_" + _sec.token_hex(32)
    th  = hash_token(raw)
    workspace_id = _resolve_workspace(request, actor)
    tok = create_api_token(body.name.strip(), th, actor.get("id") or None,
                           scope=body.scope, expires_at=expires_at, workspace_id=workspace_id)
    log_audit(actor["username"], "token.create", "token", tok["id"],
              {"name": tok["name"], "scope": tok["scope"]},
              request.client.host if request.client else None)
    return {
        "id": tok["id"], "name": tok["name"],
        "created_at": tok["created_at"],
        "scope": tok["scope"],
        "expires_at": tok["expires_at"],
        "token": raw,
    }


@router.delete("/api/tokens/{token_id}")
def api_delete_token(token_id: int, request: Request):
    actor = _require_owner(request)
    workspace_id = _resolve_workspace(request, actor)
    # grab name before deletion for audit detail — scoped to caller's workspace
    existing = next((t for t in list_api_tokens(workspace_id=workspace_id) if t["id"] == token_id), None)
    if existing is None:
        raise HTTPException(404, "Token not found")
    # enforce workspace ownership when caller has a workspace context
    if workspace_id is not None and existing.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Cross-workspace token access denied")
    delete_api_token(token_id, workspace_id=workspace_id)
    log_audit(actor["username"], "token.delete", "token", token_id,
              {"name": existing["name"] if existing else None},
              request.client.host if request.client else None)
    return {"ok": True}


# ── Forgot / reset password (owner only) ─────────────────────────────────────
class ForgotPasswordBody(BaseModel):
    email: str


@router.post("/api/auth/forgot-password")
def auth_forgot_password(body: ForgotPasswordBody):
    """Generate a one-time reset link and email it to the owner.

    Always returns 200 regardless of whether the email matched, to avoid
    leaking account existence information.
    """
    import datetime as _dt
    from app.email import send_password_reset, _is_configured
    from app.auth import hash_token
    import os

    owner = get_owner_user()
    # Only proceed if email matches the owner and email is configured
    if (
        owner
        and owner["email"].lower() == body.email.strip().lower()
        and _is_configured()
    ):
        raw_token  = _sec.token_urlsafe(32)
        token_hash = hash_token(raw_token)
        expires_at = _dt.datetime.utcnow() + _dt.timedelta(hours=1)
        try:
            create_password_reset_token(owner["id"], token_hash, expires_at)
            app_url   = os.environ.get("APP_URL", "http://localhost").rstrip("/")
            reset_url = f"{app_url}/reset-password?token={raw_token}"
            send_password_reset(
                to=owner["email"],
                reset_url=reset_url,
                username=owner["username"],
            )
        except (OSError, AttributeError, RuntimeError, redis.exceptions.RedisError) as exc:
            log.error("forgot-password: [%s] %s", type(exc).__name__, exc)

    return {"ok": True, "message": "If that email matches the owner account, a reset link has been sent."}


class ResetPasswordBody(BaseModel):
    token: str
    new_password: str


@router.post("/api/auth/reset-password")
def auth_reset_password(body: ResetPasswordBody):
    """Validate a reset token and set a new password."""
    from app.auth import hash_token, hash_password

    if len(body.new_password) < 8:
        raise HTTPException(422, "Password must be at least 8 characters")

    token_hash = hash_token(body.token)
    record     = get_password_reset_by_token(token_hash)
    if not record:
        raise HTTPException(400, "Invalid or expired reset token")

    update_user_password(record["user_id"], hash_password(body.new_password))
    consume_password_reset_token(token_hash)
    return {"ok": True, "message": "Password updated. You can now log in."}


# ── Invite accept endpoint ────────────────────────────────────────────────────
@router.get("/api/invite/accept")
def invite_accept(token: str, request: Request):
    """Redeem an invite token.

    Flow:
    1. Validate the token (not expired, not used).
    2. If the email matches an existing user → grant flow permission + auto-login.
    3. Otherwise → return 200 with {needs_signup: true, token, email, role, graph_name}
       so the frontend can redirect to a pre-filled registration form.
    """
    from app.auth import hash_token, generate_token, SESSION_COOKIE, SESSION_DAYS

    token_hash = hash_token(token)
    invite     = get_invite_by_hash(token_hash)
    if not invite:
        raise HTTPException(400, "Invalid or expired invite link")

    email      = invite["email"]
    graph_id   = invite.get("graph_id")
    role       = invite.get("role", "viewer")
    graph_name = invite.get("graph_name") or "a flow"

    # Check if the user already exists
    existing = get_user_by_email(email)
    if existing:
        # Grant permission and log them in
        if graph_id:
            set_flow_permission(existing["id"], graph_id, role)
        consume_invite_token(token_hash)
        sess_token = generate_token()
        create_session(existing["id"], hash_token(sess_token))
        from fastapi.responses import JSONResponse
        resp = JSONResponse({
            "ok": True,
            "logged_in": True,
            "graph_id": graph_id,
            "role": role,
            "graph_name": graph_name,
        })
        resp.set_cookie(SESSION_COOKIE, sess_token, httponly=True, samesite="lax",
                        max_age=SESSION_DAYS * 86400)
        return resp

    # New user — consume token atomically to prevent replay, then return
    # signup info so the frontend can show a pre-filled registration form.
    consume_invite_token(token_hash)
    return {
        "ok": True,
        "needs_signup": True,
        "token": token,        # pass back raw token for the signup form to submit
        "email": email,
        "role": role,
        "graph_id": graph_id,
        "graph_name": graph_name,
    }


class InviteSignupBody(BaseModel):
    token: str
    username: str
    password: str


@router.post("/api/invite/signup")
def invite_signup(body: InviteSignupBody, request: Request):
    """Complete signup for a new user arriving via an invite link.

    Validates the invite token, creates the account (viewer global role),
    grants the per-flow permission, consumes the token, and logs the user in.
    """
    from app.auth import hash_token, hash_password, generate_token, SESSION_COOKIE, SESSION_DAYS
    from fastapi.responses import JSONResponse

    if len(body.password) < 8:
        raise HTTPException(422, "Password must be at least 8 characters")

    token_hash = hash_token(body.token)
    invite     = get_invite_by_hash(token_hash)
    if not invite:
        raise HTTPException(400, "Invalid or expired invite link")

    email      = invite["email"]
    graph_id   = invite.get("graph_id")
    role       = invite.get("role", "viewer")

    # Double-check the email is not already registered
    if get_user_by_email(email):
        raise HTTPException(400, "An account with this email already exists. Please log in.")

    try:
        new_user = create_user(
            body.username.strip(),
            email,
            hash_password(body.password),
            role="viewer",   # global role is always viewer for invited users
        )
    except (ValueError, RuntimeError, TypeError) as exc:
        raise HTTPException(400, str(exc))

    if graph_id:
        set_flow_permission(new_user["id"], graph_id, role)

    consume_invite_token(token_hash)

    sess_token = generate_token()
    create_session(new_user["id"], hash_token(sess_token))

    log_audit("invite", "user.create", "user", new_user["id"],
              {"username": new_user["username"], "via_invite": True,
               "graph_id": graph_id, "role": role},
              request.client.host if request.client else None)

    resp = JSONResponse({
        "ok": True,
        "user": {"id": new_user["id"], "username": new_user["username"], "role": new_user["role"]},
        "graph_id": graph_id,
        "role": role,
    })
    resp.set_cookie(SESSION_COOKIE, sess_token, httponly=True, samesite="lax",
                    max_age=SESSION_DAYS * 86400)
    return resp
