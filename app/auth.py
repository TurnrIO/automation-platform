"""Authentication helpers — session-cookie based auth (v12)."""
import hashlib, secrets
import bcrypt
from fastapi import Request

from app.core.db import (
    get_user_by_id,
    get_session_by_token_hash,
    refresh_session,
)

SESSION_COOKIE = "hr_session"
SESSION_DAYS   = 30


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def generate_token() -> str:
    return secrets.token_hex(32)


def get_current_user(request: Request):
    """Return the authenticated user dict, or None if not logged in."""
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    th = hash_token(token)
    session = get_session_by_token_hash(th)
    if not session:
        return None
    refresh_session(th)
    return get_user_by_id(session["user_id"])
