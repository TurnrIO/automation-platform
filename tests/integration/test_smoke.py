"""Integration smoke test — runs against a live HiveRunr stack.

Skipped automatically unless HIVERUNR_BASE_URL is set, so it never blocks
the normal unit-test run.  In CI this is wired up as a separate job that
starts docker compose first.

Usage (local):
    HIVERUNR_BASE_URL=http://localhost docker compose up -d --build
    HIVERUNR_BASE_URL=http://localhost pytest tests/integration/ -v

Environment variables:
    HIVERUNR_BASE_URL   — base URL of the running stack (no trailing slash)
    HIVERUNR_USER       — username (default: admin)
    HIVERUNR_PASS       — password (default: adminadmin)
"""
import os
import time
import pytest
import httpx

BASE_URL = os.environ.get("HIVERUNR_BASE_URL", "").rstrip("/")
pytestmark = pytest.mark.skipif(
    not BASE_URL,
    reason="HIVERUNR_BASE_URL not set — skipping integration tests",
)

USERNAME = os.environ.get("HIVERUNR_USER", "admin")
PASSWORD = os.environ.get("HIVERUNR_PASS", "adminadmin")


@pytest.fixture(scope="module")
def client():
    """Authenticated httpx client for the test session."""
    c = httpx.Client(base_url=BASE_URL, follow_redirects=True, timeout=30)
    resp = c.post("/api/auth/login", json={"username": USERNAME, "password": PASSWORD})
    assert resp.status_code == 200, f"Login failed: {resp.text}"
    return c


def _poll_run(client, task_id, timeout=120):
    """Poll /api/runs/by-task/{task_id} until the run reaches a terminal state."""
    for i in range(timeout):
        r = client.get(f"/api/runs/by-task/{task_id}")
        if r.status_code == 200:
            status = r.json().get("status")
            if status in ("succeeded", "failed"):
                return r.json()
        time.sleep(1)
    raise AssertionError(f"Run {task_id} did not complete within {timeout}s")


# ── health ────────────────────────────────────────────────────────────────────

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json().get("status") == "ok"


# ── auth ──────────────────────────────────────────────────────────────────────

def test_auth_status(client):
    resp = client.get("/api/auth/status")
    assert resp.status_code == 200
    data = resp.json()
    # endpoint returns "logged_in", not "authenticated"
    assert data.get("logged_in") is True


def test_unauthenticated_redirects_to_login():
    c = httpx.Client(base_url=BASE_URL, follow_redirects=False, timeout=10)
    resp = c.get("/")
    assert resp.status_code in (302, 307)
    assert "/login" in resp.headers.get("location", "")


# ── graph CRUD + run ──────────────────────────────────────────────────────────

# GraphCreate expects graph_data (dict), not graph_json (string)
_SMOKE_GRAPH = {
    "name": "smoke-test-graph",
    "graph_data": {
        "nodes": [
            {"id": "t1", "type": "trigger.manual", "data": {"config": {}}},
            {"id": "l1", "type": "action.log",     "data": {"config": {"message": "smoke ok"}}},
        ],
        "edges": [{"source": "t1", "target": "l1"}],
    },
}


@pytest.fixture(scope="module")
def graph_id(client):
    resp = client.post("/api/graphs", json=_SMOKE_GRAPH)
    assert resp.status_code == 200, f"Create graph failed: {resp.text}"
    gid = resp.json().get("id")
    assert gid, f"No id in response: {resp.json()}"
    yield gid
    client.delete(f"/api/graphs/{gid}")


def test_graph_created(graph_id):
    assert graph_id is not None


def test_graph_appears_in_list(client, graph_id):
    resp = client.get("/api/graphs")
    assert resp.status_code == 200
    data = resp.json()
    # API returns {"graphs": [...], "pagination": {...}} with pagination
    graphs = data if isinstance(data, list) else data.get("graphs", [])
    ids = [g.get("id") for g in graphs]
    assert graph_id in ids


def test_manual_run_succeeds(client, graph_id):
    resp = client.post(f"/api/graphs/{graph_id}/run", json={})
    assert resp.status_code == 200
    task_id = resp.json().get("task_id")
    assert task_id

    run = _poll_run(client, task_id)
    assert run["status"] == "succeeded", f"Run ended with status={run['status']}"


def test_run_has_traces(client, graph_id):
    resp = client.post(f"/api/graphs/{graph_id}/run", json={})
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]

    run = _poll_run(client, task_id)
    traces = run.get("traces") or []
    assert len(traces) > 0, "Expected at least one trace"
    assert all(t.get("status") in ("ok", "skipped") for t in traces)
