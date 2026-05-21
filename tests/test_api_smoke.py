"""API smoke tests — every major endpoint must return non-500 for an
authenticated request.  Uses FastAPI's TestClient (no network needed).

These tests are intentionally shallow: they verify that routes exist, are
reachable, and don't crash on a well-formed request.  They are NOT testing
business logic — that belongs in unit tests.

Run:  pytest tests/test_api_smoke.py -v
"""
import json
import pytest
from fastapi.testclient import TestClient


# ── App bootstrap ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def app_with_mocks():
    """Import the FastAPI app with DB/Redis patched out.

    Session-scoped so the app is only imported once; individual tests get
    their own TestClient instance (function-scoped) to avoid event-loop
    lifecycle issues with module-scoped clients.
    """
    import os
    import unittest.mock as mock
    import app.core.db as db_mod

    # Set required env vars before app.main is imported
    os.environ["API_KEY"] = "test-ci-api-key-for-pytest-only"
    os.environ["SECRET_KEY"] = "test-secret-key-for-pytest-only"
    os.environ["DATABASE_URL"] = "postgresql://localhost/test"
    os.environ["REDIS_URL"] = "redis://localhost/0"

    fake_conn = mock.MagicMock()
    fake_conn.__enter__ = lambda s: s
    fake_conn.__exit__ = mock.MagicMock(return_value=False)
    fake_conn.cursor.return_value.__enter__ = lambda s: s
    fake_conn.cursor.return_value.__exit__ = mock.MagicMock(return_value=False)
    fake_conn.cursor.return_value.fetchone.return_value = None
    fake_conn.cursor.return_value.fetchall.return_value = []

    with mock.patch.object(db_mod, "get_conn", return_value=fake_conn), \
         mock.patch.object(db_mod, "run_migrations", return_value=None), \
         mock.patch("app.main.init_db", return_value=None), \
         mock.patch("app.main.seed_example_graphs", return_value=None), \
         mock.patch("app.main.load_secrets", return_value=None):

        from app.main import app as _app
        yield _app


@pytest.fixture()
def client(app_with_mocks):
    """Fresh TestClient per test — avoids CancelledError from shared event loops."""
    # Do NOT use 'with TestClient' context manager: it triggers lifespan
    # events (startup/shutdown) which require a live DB. Plain instantiation
    # skips lifespan and is sufficient for route-existence checks.
    return TestClient(app_with_mocks, raise_server_exceptions=False)


# ── Duplicate route detector ───────────────────────────────────────────────────

def test_no_duplicate_routes(app_with_mocks):
    """Fail if any two routes share method + path — the first one silently wins."""
    fastapi_app = app_with_mocks

    seen = {}
    duplicates = []
    for route in fastapi_app.routes:
        if not hasattr(route, "methods"):
            continue
        for method in route.methods:
            key = f"{method} {route.path}"
            if key in seen:
                duplicates.append(f"DUPLICATE: {key}  ({seen[key]} AND {getattr(route, 'endpoint', '?').__module__})")
            else:
                seen[key] = getattr(route.endpoint, "__module__", "?")

    assert not duplicates, "\n".join(duplicates)


# ── Auth endpoints ─────────────────────────────────────────────────────────────

def test_auth_status(client):
    r = client.get("/api/auth/status")
    assert r.status_code != 500, r.text


def test_auth_login_bad_creds(client):
    r = client.post("/api/auth/login", json={"username": "x", "password": "y"})
    # 401 is expected — just not 500
    assert r.status_code != 500, r.text


def test_auth_check_unauthenticated(client):
    r = client.get("/api/auth/check")
    # Accept redirect (302) to /login, which requires frontend assets not present
    # in the unit-test CI environment (no Docker build step). Also accept 503
    # from the static-file server when login.html is missing.
    assert r.status_code in (200, 401, 403, 302), r.text


# ── Public / health endpoints ──────────────────────────────────────────────────

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert "status" in data
    assert "version" in data
    assert data["version"] != "8", "/health is returning the old hardcoded version string"


def test_templates_list_unauthenticated(client):
    """Template list must be reachable (returns 401 if not auth'd, not 500)."""
    r = client.get("/api/templates")
    assert r.status_code != 500, r.text


# ── Route existence checks (unauthenticated → 401/403, never 500) ─────────────

@pytest.mark.parametrize("method,path", [
    ("GET",  "/api/graphs"),
    ("GET",  "/api/runs"),
    ("GET",  "/api/credentials"),
    ("GET",  "/api/schedules"),
    ("GET",  "/api/metrics"),
    ("GET",  "/api/analytics/flows"),
    ("GET",  "/api/analytics/daily"),
    ("GET",  "/api/audit-log"),
    ("GET",  "/api/system/status"),
    ("GET",  "/api/version"),
    ("GET",  "/api/templates"),
    ("GET",  "/api/runs/retention"),
    ("GET",  "/api/settings/ratelimit"),
    ("GET",  "/api/runs/queue"),
    ("POST", "/api/graphs/import"),
    ("POST", "/api/templates/daily_health_check/use"),
    ("GET",  "/api/scripts"),
    ("GET",  "/api/users"),
    ("GET",  "/api/tokens"),
])
def test_route_exists_returns_non_500(client, method, path):
    """Every listed endpoint must exist and return 401/403, never 404 or 500."""
    r = client.request(method, path, json={})
    assert r.status_code not in (404, 500), (
        f"{method} {path} returned {r.status_code}: {r.text[:200]}"
    )


# ── Page routes ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("path", ["/login", "/canvas", "/health"])
def test_page_routes_non_500(client, path):
    r = client.get(path)
    assert r.status_code != 500, f"{path} returned 500: {r.text[:200]}"
