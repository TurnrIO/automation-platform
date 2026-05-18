"""Rate limiting via slowapi + Redis storage.

Provides a Limiter instance and shared Redis storage for distributed
rate limiting across all uvicorn workers.

Usage in routers:
    from app.routers._rate_limit import limiter

    @router.post("/login")
    @limiter.limit("10/minute")
    async def login(request: Request, ...):
        ...

    On RateLimitExceeded, slowapi automatically returns a 429 with JSON body.
    Optionally register a custom handler in main.py:
        from slowapi.errors import RateLimitExceeded
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
"""
import os
from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded


def _client_key(request: Request) -> str:
    """Extract client IP, respecting X-Forwarded-For from Caddy proxy."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return get_remote_address(request)


# ── Storage ───────────────────────────────────────────────────────────────────
# Redis-backed storage so rate limits are shared across all worker processes.
# Falls back to in-memory if Redis is unavailable.
_storage_uri = "memory://"

_redis_url = os.environ.get("REDIS_URL", "")
if _redis_url:
    try:
        # Use Redis DB 1 for rate limiting (DB 0 is Celery's broker/result backend)
        _storage_uri = _redis_url.rstrip("/") + "/1"
    except Exception:
        _storage_uri = "memory://"

limiter = Limiter(key_func=_client_key, default_limits=[], storage_uri=_storage_uri)


async def _rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Custom JSON handler for RateLimitExceeded — cleaner than slowapi's default."""
    return JSONResponse(
        status_code=429,
        content={"detail": str(exc.detail) if hasattr(exc, "detail") else "Rate limit exceeded"},
        headers={
            "Retry-After": "60",
            "X-RateLimit-Policy": "HiveRunr",
        },
    )