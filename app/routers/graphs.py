"""Graphs, graph versions, and graph-run routers."""
import json
from json import JSONDecodeError
import psycopg2
import logging
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from app.deps import _check_admin, _check_flow_access, _resolve_workspace, ROLE_LEVELS
from app.core.db import (
    list_graphs, create_graph, get_graph, update_graph, delete_graph,
    get_graph_by_slug, duplicate_graph, count_graphs,
    list_graph_versions, save_graph_version, get_graph_version,
    sync_graph_schedules,
    get_graph_alerts, update_graph_alerts,
    log_audit,
    get_user_by_id, list_users,
    list_flow_permissions, set_flow_permission, delete_flow_permission,
    get_permitted_graph_ids, FLOW_ROLE_LEVELS,
    create_invite_token,
)
from app.worker import enqueue_graph
from app.core.executor import run_graph

log = logging.getLogger(__name__)
router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _graph_with_data(g):
    if not g:
        return None
    try:
        gd = json.loads(g.get('graph_json') or '{}')
    except JSONDecodeError:
        gd = {}
    return {**{k: v for k, v in g.items() if k != 'graph_json'}, 'graph_data': gd}


def _sync_cron_triggers(graph_id: int, graph_data: dict):
    try:
        nodes = (graph_data or {}).get('nodes', [])
        sync_graph_schedules(graph_id, [n for n in nodes if n.get('type') == 'trigger.cron'])
    except (AttributeError, RuntimeError, OSError) as exc:
        log.warning(f"Could not sync schedules for graph {graph_id}: {exc}")


def _is_admin_or_owner(user: dict) -> bool:
    return ROLE_LEVELS.get(user.get("role", "viewer"), 0) >= 1


# ── Graph CRUD ────────────────────────────────────────────────────────────────
class GraphCreate(BaseModel):
    name: str; description: str = ""; graph_data: dict = {}
    tags: list[str] = []


class GraphUpdate(BaseModel):
    name: Optional[str] = None; description: Optional[str] = None
    graph_data: Optional[dict] = None; enabled: Optional[bool] = None
    tags: Optional[list[str]] = None; priority: Optional[int] = None
    pinned: Optional[bool] = None
    save_note: Optional[str] = None   # optional note stored on the graph_version snapshot


@router.get("/api/graphs")
def api_graphs(request: Request, page: int = 1, page_size: int = 50):
    """List graphs for the caller's workspace with pagination."""
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    page = max(1, page)
    page_size = min(max(1, page_size), 100)
    all_graphs = list_graphs(workspace_id=workspace_id, page=page, page_size=page_size)
    # Viewer-role users only see flows they have explicit permission for.
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        permitted = set(get_permitted_graph_ids(user["id"]))
        all_graphs = [g for g in all_graphs if g["id"] in permitted]
    total = count_graphs(workspace_id=workspace_id)
    return {
        "graphs": [_graph_with_data(g) for g in all_graphs],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": (total + page_size - 1) // page_size if total else 1,
        },
    }


@router.post("/api/graphs")
def api_graph_create(body: GraphCreate, request: Request):
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Creating flows requires admin or owner role")
    workspace_id = _resolve_workspace(request, user)
    g = create_graph(body.name, body.description, json.dumps(body.graph_data), workspace_id=workspace_id, tags=body.tags)
    _sync_cron_triggers(g['id'], body.graph_data)
    save_graph_version(g['id'], body.name, json.dumps(body.graph_data), note="Initial version")
    log_audit(user["username"], "graph.create", "graph", g["id"],
              {"name": body.name, "description": body.description},
              request.client.host if request.client else None)
    return _graph_with_data(g)


# Maximum import payload size: 10 MB
GRAPH_IMPORT_MAX_BYTES = 10 * 1024 * 1024

@router.post("/api/graphs/import")
async def api_graph_import(request: Request):
    """Import a flow from a JSON bundle.

    Accepts either a full HiveRunr export bundle (with hiverunr_export header)
    or a minimal object: { name, description?, graph_data }.
    Creates a new graph in the current workspace and saves an initial version.
    """
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Importing flows requires admin or owner role")
    workspace_id = _resolve_workspace(request, user)
    content_length = request.headers.get("content-length")
    try:
        if content_length and int(content_length) > GRAPH_IMPORT_MAX_BYTES:
            raise HTTPException(413, f"Payload too large — maximum {GRAPH_IMPORT_MAX_BYTES // (1024*1024)} MB")
    except ValueError:
        raise HTTPException(400, "Invalid Content-Length header")
    try:
        body = await request.json()
    except JSONDecodeError:
        raise HTTPException(400, "Invalid JSON body")

    # Reject suspiciously large parsed bodies even if Content-Length was not set
    if len(json.dumps(body)) > GRAPH_IMPORT_MAX_BYTES:
        raise HTTPException(413, f"Payload too large — maximum {GRAPH_IMPORT_MAX_BYTES // (1024*1024)} MB")

    name = (body.get("name") or "Imported Flow").strip()
    desc = body.get("description") or ""
    gd   = body.get("graph_data") or {}

    if not isinstance(gd, dict):
        raise HTTPException(400, "graph_data must be a JSON object")

    g = create_graph(name, desc, json.dumps(gd), workspace_id=workspace_id)
    _sync_cron_triggers(g["id"], gd)
    save_graph_version(g["id"], name, json.dumps(gd), note="Imported")
    log_audit(user["username"], "graph.import", "graph", g["id"],
              {"name": name},
              request.client.host if request.client else None)
    return _graph_with_data(g)


@router.get("/api/graphs/by-slug/{slug}")
def api_graph_by_slug(slug: str, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    g = get_graph_by_slug(slug, workspace_id=workspace_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, g["id"], "viewer")
    return _graph_with_data(g)


@router.get("/api/graphs/{graph_id}")
def api_graph_get(graph_id: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if workspace_id is not None and g.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Access denied to this workspace's graph")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "viewer")
    return _graph_with_data(g)


@router.put("/api/graphs/{graph_id}")
def api_graph_update(graph_id: int, body: GraphUpdate, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if workspace_id is not None and g.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Access denied to this workspace's graph")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "editor")
    update_graph(graph_id, name=body.name, description=body.description,
                 graph_json=json.dumps(body.graph_data) if body.graph_data is not None else None,
                 enabled=body.enabled, tags=body.tags,
                 priority=body.priority, pinned=body.pinned)
    if body.graph_data is not None:
        _sync_cron_triggers(graph_id, body.graph_data)
        gname = body.name or g['name']
        save_graph_version(graph_id, gname, json.dumps(body.graph_data),
                           note=body.save_note or "")
    log_audit(user["username"], "graph.update", "graph", graph_id,
              {"name": body.name or g["name"]},
              request.client.host if request.client else None)
    return _graph_with_data(get_graph(graph_id))


@router.delete("/api/graphs/{graph_id}")
def api_graph_delete(graph_id: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Deleting flows requires admin or owner role")
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if workspace_id is not None and g.get("workspace_id") != workspace_id:
        raise HTTPException(403, "Access denied to this workspace's graph")
    delete_graph(graph_id)
    log_audit(user["username"], "graph.delete", "graph", graph_id,
              {"name": g["name"]},
              request.client.host if request.client else None)
    return {"deleted": True, "id": graph_id}


@router.post("/api/graphs/reseed")
def api_graphs_reseed(request: Request):
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "This action requires admin or owner role")
    # Import seeding from main to avoid circular deps — seed logic lives on app startup
    from app.seeds import seed_example_graphs
    n = seed_example_graphs()
    return {"seeded": n, "message": f"Re-seeded {n} missing example flow(s)"}


# ── Single-node test ─────────────────────────────────────────────────────────
class NodeTestBody(BaseModel):
    input: dict = {}


@router.post("/api/graphs/{graph_id}/nodes/{node_id}/test")
def api_test_node(graph_id: int, node_id: str, body: NodeTestBody, request: Request):
    """Execute a single node synchronously and return its output.

    Loads the graph and credentials, finds the node, and calls its handler
    directly with the provided input.  No Celery task, no run record — just
    an instant test invocation that populates the NodeIOPanel in the canvas.
    """
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if workspace_id is not None and g.get('workspace_id') != workspace_id:
        raise HTTPException(403, "Graph not in your workspace")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "runner")

    try:
        graph_data = json.loads(g.get('graph_json') or '{}')
    except JSONDecodeError:
        graph_data = {}

    nodes_map = {n['id']: n for n in graph_data.get('nodes', [])}
    edges     = graph_data.get('edges', [])
    node      = nodes_map.get(node_id)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found in graph")

    ntype = node.get('type', node.get('data', {}).get('type', ''))
    if ntype in ('note',):
        raise HTTPException(400, "Cannot test UI-only nodes")

    try:
        from app.core.db import load_all_credentials
        creds = load_all_credentials(workspace_id=g.get("workspace_id"))
    except (ImportError, psycopg2.Error):
        creds = {}

    from app.core.executor import run_one_node

    messages = []
    result = run_one_node(
        node=node,
        inp=body.input,
        context={node_id: body.input},
        creds=creds,
        logger=messages.append,
        edges=edges,
        nodes_map=nodes_map,
    )
    result["node_id"]   = node_id
    result["node_type"] = ntype
    result["logs"]      = messages
    return result


# ── Graph run ─────────────────────────────────────────────────────────────────
@router.post("/api/graphs/{graph_id}/run")
async def api_graph_run(graph_id: int, request: Request):
    user = _check_admin(request)
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    workspace_id = _resolve_workspace(request, user)
    if workspace_id is not None and g.get('workspace_id') != workspace_id:
        raise HTTPException(403, "Graph not in your workspace")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "runner")
    try:
        body = await request.json()
    except JSONDecodeError:
        body = {}
    payload        = (body or {}).get("payload") or body or {"source": "api"}
    start_node_id  = (body or {}).get("start_node_id")   # "run from this node" feature
    prior_context  = (body or {}).get("prior_context")   # previous run's node outputs
    flow_priority  = g.get("priority", 5) if isinstance(g, dict) else 5

    task_kwargs = {}
    if start_node_id:
        task_kwargs = {"start_node_id": start_node_id, "prior_context": prior_context or {}}

    task_id = None
    try:
        task = enqueue_graph.apply_async(
            args=[graph_id, payload], kwargs=task_kwargs, priority=flow_priority
        )
        task_id = task.id
    except (OSError, RuntimeError, AttributeError) as exc:
        log.warning("Celery unavailable (%s) — running graph inline", exc)
        # Fall back to inline execution so the API still responds
        import uuid
        task_id = str(uuid.uuid4())
        try:
            from app.core.db import update_run, init_db
            init_db()
            update_run(task_id, "running")
        except (OSError, RuntimeError) as init_err:
            log.warning("Could not initialize inline run: %s", init_err)
            raise HTTPException(500, f"Graph run failed: {init_err}")
        try:
            graph_data = json.loads(g.get('graph_json') or '{}')
        except JSONDecodeError:
            graph_data = {}
        try:
            result = run_graph(
                graph_data,
                payload,
                workspace_id=g.get('workspace_id'),
                start_node_id=start_node_id,
                prior_context=prior_context,
            )
            update_run(task_id, "succeeded", result=result,
                       traces=result.get('traces', []))
        except (OSError, psycopg2.Error) as inline_err:
            log.exception("Inline graph run failed")
            update_run(task_id, "failed", result={"error": str(inline_err)})
            raise HTTPException(500, f"Graph run failed: {inline_err}")

    try:
        from app.core.db import get_conn
        with get_conn() as conn:
            conn.cursor().execute(
                "INSERT INTO runs(task_id, graph_id, status, initial_payload, workspace_id) VALUES(%s,%s,'queued',%s,%s)",
                (task_id, graph_id, json.dumps(payload), workspace_id)
            )
    except psycopg2.Error as e:
        log.warning(f"Could not pre-create run record: {e}")
    log_audit(user["username"], "graph.run", "graph", graph_id,
              {"name": g["name"], "task_id": task_id},
              request.client.host if request.client else None)
    return {"queued": True, "task_id": task_id, "graph": g["name"]}


# ── Graph versions ────────────────────────────────────────────────────────────
@router.get("/api/graphs/{graph_id}/versions")
def api_graph_versions(graph_id: int, request: Request):
    user = _check_admin(request)
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "viewer")
    return list_graph_versions(graph_id)


@router.post("/api/graphs/{graph_id}/duplicate")
def api_duplicate_graph(graph_id: int, request: Request):
    """Clone a graph with a unique '(copy)' name suffix."""
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Duplicating flows requires admin or owner role")
    try:
        new_g = duplicate_graph(graph_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    log_audit(user["username"], "graph.duplicate", "graph", graph_id,
              {"new_id": new_g["id"], "new_name": new_g["name"]},
              request.client.host if request.client else None)
    return _graph_with_data(new_g)


@router.get("/api/graphs/{graph_id}/versions/{version_id}")
def api_get_graph_version(graph_id: int, version_id: int, request: Request):
    """Return the full graph_json for a specific version (read-only preview)."""
    user = _check_admin(request)
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "viewer")
    v = get_graph_version(version_id)
    if not v or v['graph_id'] != graph_id:
        raise HTTPException(404, "Version not found")
    try:
        gd = json.loads(v.get('graph_json') or '{}')
    except JSONDecodeError:
        gd = {}
    return {**{k: val for k, val in v.items() if k != 'graph_json'}, 'graph_data': gd}


@router.post("/api/graphs/{graph_id}/versions/{version_id}/restore")
def api_restore_version(graph_id: int, version_id: int, request: Request):
    user = _check_admin(request)
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "editor")
    v = get_graph_version(version_id)
    if not v or v['graph_id'] != graph_id:
        raise HTTPException(404, "Version not found")
    update_graph(graph_id, graph_json=v['graph_json'])
    try:
        gd = json.loads(v['graph_json'])
    except JSONDecodeError:
        gd = {}
    _sync_cron_triggers(graph_id, gd)
    save_graph_version(graph_id, g['name'], v['graph_json'], note=f"Restored from v{v['version']}")
    log_audit(user["username"], "graph.restore_version", "graph", graph_id,
              {"name": g["name"], "version_id": version_id, "version": v.get("version")},
              request.client.host if request.client else None)
    return _graph_with_data(get_graph(graph_id))


# ── Per-flow alert configuration ──────────────────────────────────────────────
class AlertConfig(BaseModel):
    alert_emails:       Optional[str]  = None   # comma-separated
    alert_webhook:      Optional[str]  = None
    alert_on_success:   bool           = False
    alert_min_failures: int            = 1      # only alert after N consecutive failures


@router.get("/api/graphs/{graph_id}/alerts")
def api_get_alerts(graph_id: int, request: Request):
    user = _check_admin(request)
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "editor")
    cfg = get_graph_alerts(graph_id)
    if cfg is None:
        raise HTTPException(404, "Graph not found")
    return cfg


@router.put("/api/graphs/{graph_id}/alerts")
def api_update_alerts(graph_id: int, body: AlertConfig, request: Request):
    user = _check_admin(request)
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    if not _is_admin_or_owner(user) and user.get("id", 0) != 0:
        _check_flow_access(request, graph_id, "editor")
    update_graph_alerts(
        graph_id,
        alert_emails=body.alert_emails,
        alert_webhook=body.alert_webhook,
        alert_on_success=body.alert_on_success,
        alert_min_failures=body.alert_min_failures,
    )
    return get_graph_alerts(graph_id)


# ── Per-flow permissions ───────────────────────────────────────────────────────
@router.get("/api/graphs/{graph_id}/permissions")
def api_get_permissions(graph_id: int, request: Request):
    """List all user permissions for this flow (admin/owner only)."""
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Managing permissions requires admin or owner role")
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    perms = list_flow_permissions(graph_id)
    # Also return all users (for the "add user" dropdown)
    all_users = [
        {"id": u["id"], "username": u["username"], "email": u["email"], "role": u["role"]}
        for u in list_users()
        if u.get("role") not in ("owner",)   # owner doesn't need per-flow perms
    ]
    return {"permissions": perms, "users": all_users}


class SetPermissionBody(BaseModel):
    user_id: int
    role: str   # viewer | runner | editor


@router.put("/api/graphs/{graph_id}/permissions")
def api_set_permission(graph_id: int, body: SetPermissionBody, request: Request):
    """Grant or update a user's role on this flow (admin/owner only)."""
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Managing permissions requires admin or owner role")
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    if body.role not in FLOW_ROLE_LEVELS:
        raise HTTPException(422, f"role must be one of: {list(FLOW_ROLE_LEVELS)}")
    target = get_user_by_id(body.user_id)
    if not target:
        raise HTTPException(404, "User not found")
    if target.get("role") == "owner":
        raise HTTPException(400, "Cannot set per-flow permissions for the owner")
    actor_id = user.get("id") or None
    if actor_id == 0:
        actor_id = None
    set_flow_permission(body.user_id, graph_id, body.role, granted_by=actor_id)
    log_audit(user["username"], "flow.permission.set", "graph", graph_id,
              {"user_id": body.user_id, "username": target["username"], "role": body.role},
              request.client.host if request.client else None)
    return {"ok": True, "user_id": body.user_id, "graph_id": graph_id, "role": body.role}


@router.delete("/api/graphs/{graph_id}/permissions/{user_id}")
def api_delete_permission(graph_id: int, user_id: int, request: Request):
    """Remove a user's access to this flow (admin/owner only)."""
    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Managing permissions requires admin or owner role")
    if not get_graph(graph_id):
        raise HTTPException(404, "Graph not found")
    delete_flow_permission(user_id, graph_id)
    log_audit(user["username"], "flow.permission.delete", "graph", graph_id,
              {"user_id": user_id},
              request.client.host if request.client else None)
    return {"ok": True}


# ── Flow invite (send link by email) ─────────────────────────────────────────
class InviteBody(BaseModel):
    email: str
    role: str = "viewer"


@router.post("/api/graphs/{graph_id}/invite")
def api_invite_to_flow(graph_id: int, body: InviteBody, request: Request):
    """Send an email invite link that grants access to this flow.

    If the email matches an existing user, the invite grants them the requested
    role directly.  Otherwise, following the link creates a new viewer/runner
    account scoped to this flow.
    """
    import secrets as _sec
    import datetime as _dt
    import os
    from app.email import _is_configured
    from app.auth import hash_token

    user = _check_admin(request)
    if not _is_admin_or_owner(user):
        raise HTTPException(403, "Inviting users requires admin or owner role")
    g = get_graph(graph_id)
    if not g:
        raise HTTPException(404, "Graph not found")
    if body.role not in FLOW_ROLE_LEVELS:
        raise HTTPException(422, f"role must be one of: {list(FLOW_ROLE_LEVELS)}")

    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(422, "Invalid email address")

    raw_token  = _sec.token_urlsafe(32)
    token_hash = hash_token(raw_token)
    expires_at = _dt.datetime.utcnow() + _dt.timedelta(days=7)
    actor_id = user.get("id") or None
    if actor_id == 0:
        actor_id = None

    app_url    = os.environ.get("APP_URL", "http://localhost").rstrip("/")
    invite_url = f"{app_url}/invite/accept?token={raw_token}"

    sent = False
    if _is_configured():
        try:
            from app.email import send_flow_invite
            send_flow_invite(
                to=email,
                invite_url=invite_url,
                flow_name=g["name"],
                role=body.role,
                invited_by=user["username"],
            )
            sent = True
        except (OSError, RuntimeError, AttributeError) as exc:
            log.warning("Could not send invite email: %s", exc)
            raise HTTPException(500, "Failed to send invite email. Please try again.")

    # Create the token only after confirmed email send (so a failed send cannot
    # leave a permanently-unredeemable token), or when email is not configured
    # (inviter must share the URL manually).
    create_invite_token(token_hash, email, graph_id, body.role, actor_id, expires_at)

    log_audit(user["username"], "flow.invite", "graph", graph_id,
              {"email": email, "role": body.role, "sent": sent},
              request.client.host if request.client else None)
    return {
        "ok": True,
        "email": email,
        "role": body.role,
        "invite_url": invite_url,
        "email_sent": sent,
        "expires_at": expires_at.isoformat(),
    }

