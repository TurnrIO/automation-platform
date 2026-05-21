"""Workspace CRUD and member management router.

All endpoints require authentication.  Owner/super-admin operations (create,
delete, list-all) require owner role.  Workspace-level admin operations
(rename, manage members) require admin or owner *within that workspace*.

In W1 the workspace tables exist but no resources (graphs/runs/etc.) are
scoped to them yet.  That happens in W2/W3.  This router provides the full
management surface now so the UI can be built and tested immediately.
"""
import logging
import re
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from app.deps import _check_admin, _require_owner
from app.core.db import (
    create_workspace, get_workspace, get_workspace_by_slug,
    list_workspaces, update_workspace, delete_workspace,
    list_workspace_members, get_workspace_member,
    set_workspace_member, remove_workspace_member,
    list_user_workspaces, get_user_by_id, log_audit, WORKSPACE_ROLE_LEVELS, _slugify,
)

log = logging.getLogger(__name__)
router = APIRouter()

VALID_PLANS = ("free", "pro", "enterprise")


def _require_workspace_admin(request: Request, workspace_id: int) -> dict:
    """Allow global owner OR a workspace admin/owner to manage a workspace."""
    user = _check_admin(request)
    global_role = user.get("role", "viewer")
    if global_role == "owner":
        return user   # global owner can manage any workspace
    # Check workspace membership
    member = get_workspace_member(workspace_id, user.get("id", -1))
    if not member or WORKSPACE_ROLE_LEVELS.get(member["role"], 0) < 1:
        raise HTTPException(403, "Workspace admin or owner role required")
    return user


# ── Workspace CRUD ─────────────────────────────────────────────────────────────
class WorkspaceCreate(BaseModel):
    name: str
    slug: Optional[str] = None
    plan: str = "free"


class WorkspaceUpdate(BaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    plan: Optional[str] = None


@router.get("/api/workspaces")
def api_list_workspaces(request: Request):
    """List all workspaces (global owner only) or just the caller's workspaces."""
    user = _check_admin(request)
    if user.get("role") == "owner":
        return list_workspaces()
    # Regular users: only return workspaces they belong to.
    uid = user.get("id", 0)
    if uid == 0:
        return []
    return list_user_workspaces(uid)


@router.post("/api/workspaces")
def api_create_workspace(body: WorkspaceCreate, request: Request):
    """Create a new workspace (owner only).

    The calling user is automatically added as the workspace owner.
    """
    user = _require_owner(request)
    if not body.name.strip():
        raise HTTPException(422, "Workspace name is required")
    if body.plan not in VALID_PLANS:
        raise HTTPException(422, f"plan must be one of {list(VALID_PLANS)}")

    # Validate / auto-generate slug
    slug = body.slug.strip() if body.slug else _slugify(body.name)
    if not re.match(r"^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$", slug):
        raise HTTPException(422, "slug must be lowercase alphanumeric with hyphens (2–63 chars)")
    if get_workspace_by_slug(slug):
        raise HTTPException(409, f"Workspace slug '{slug}' is already taken")

    try:
        ws = create_workspace(body.name, slug=slug, plan=body.plan)
    except (AttributeError, TypeError, RuntimeError) as exc:
        raise HTTPException(400, str(exc))

    # Add the creator as workspace owner
    uid = user.get("id")
    if uid:
        set_workspace_member(ws["id"], uid, "owner")

    log_audit(user["username"], "workspace.create", "workspace", ws["id"],
              {"name": ws["name"], "slug": ws["slug"]},
              request.client.host if request.client else None)
    ws["member_count"] = 1
    return ws


@router.get("/api/workspaces/{workspace_id}")
def api_get_workspace(workspace_id: int, request: Request):
    user = _check_admin(request)
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(404, "Workspace not found")
    # Non-owners can only see workspaces they belong to
    if user.get("role") != "owner":
        member = get_workspace_member(workspace_id, user.get("id", -1))
        if not member:
            raise HTTPException(403, "You are not a member of this workspace")
    return ws


@router.patch("/api/workspaces/{workspace_id}")
def api_update_workspace(workspace_id: int, body: WorkspaceUpdate, request: Request):
    user = _require_workspace_admin(request, workspace_id)
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(404, "Workspace not found")
    if body.plan is not None and body.plan not in VALID_PLANS:
        raise HTTPException(422, f"plan must be one of {list(VALID_PLANS)}")
    if body.slug is not None:
        new_slug = body.slug.strip()
        if not re.match(r"^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$", new_slug):
            raise HTTPException(422, "slug must be lowercase alphanumeric with hyphens")
        existing = get_workspace_by_slug(new_slug)
        if existing and existing["id"] != workspace_id:
            raise HTTPException(409, f"Workspace slug '{new_slug}' is already taken")
    updated = update_workspace(workspace_id, name=body.name, slug=body.slug, plan=body.plan)
    log_audit(user["username"], "workspace.update", "workspace", workspace_id,
              {"name": body.name, "slug": body.slug, "plan": body.plan},
              request.client.host if request.client else None)
    return updated


@router.delete("/api/workspaces/{workspace_id}")
def api_delete_workspace(workspace_id: int, request: Request):
    """Delete a workspace (global owner only).  Cannot delete the default workspace."""
    user = _require_owner(request)
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(404, "Workspace not found")
    if ws["slug"] == "default":
        raise HTTPException(400, "Cannot delete the default workspace")
    delete_workspace(workspace_id)
    log_audit(user["username"], "workspace.delete", "workspace", workspace_id,
              {"name": ws["name"], "slug": ws["slug"]},
              request.client.host if request.client else None)
    return {"deleted": True, "id": workspace_id}


# ── Member management ──────────────────────────────────────────────────────────
@router.get("/api/workspaces/{workspace_id}/members")
def api_list_members(workspace_id: int, request: Request):
    user = _check_admin(request)
    if not get_workspace(workspace_id):
        raise HTTPException(404, "Workspace not found")
    if user.get("role") != "owner":
        member = get_workspace_member(workspace_id, user.get("id", -1))
        if not member:
            raise HTTPException(403, "You are not a member of this workspace")
    return list_workspace_members(workspace_id)


class SetMemberBody(BaseModel):
    user_id: int
    role: str = "viewer"


@router.put("/api/workspaces/{workspace_id}/members")
def api_set_member(workspace_id: int, body: SetMemberBody, request: Request):
    """Add or update a user's role in this workspace."""
    user = _require_workspace_admin(request, workspace_id)
    if not get_workspace(workspace_id):
        raise HTTPException(404, "Workspace not found")
    if body.role not in WORKSPACE_ROLE_LEVELS:
        raise HTTPException(422, f"role must be one of: {list(WORKSPACE_ROLE_LEVELS)}")
    target = get_user_by_id(body.user_id)
    if not target:
        raise HTTPException(404, "User not found")
    # Only global owner can set workspace-owner role
    if body.role == "owner" and user.get("role") != "owner":
        raise HTTPException(403, "Only the global owner can grant workspace owner role")
    set_workspace_member(workspace_id, body.user_id, body.role)
    log_audit(user["username"], "workspace.member.set", "workspace", workspace_id,
              {"user_id": body.user_id, "username": target["username"], "role": body.role},
              request.client.host if request.client else None)
    return {"ok": True, "workspace_id": workspace_id, "user_id": body.user_id, "role": body.role}


@router.delete("/api/workspaces/{workspace_id}/members/{user_id}")
def api_remove_member(workspace_id: int, user_id: int, request: Request):
    """Remove a user from a workspace."""
    user = _require_workspace_admin(request, workspace_id)
    if not get_workspace(workspace_id):
        raise HTTPException(404, "Workspace not found")
    # Can't remove yourself if you're the last owner
    if user.get("id") == user_id:
        owners = [m for m in list_workspace_members(workspace_id) if m["role"] == "owner"]
        if len(owners) <= 1 and any(m["user_id"] == user_id for m in owners):
            raise HTTPException(400, "Cannot remove the last owner from a workspace")
    remove_workspace_member(workspace_id, user_id)
    log_audit(user["username"], "workspace.member.remove", "workspace", workspace_id,
              {"user_id": user_id},
              request.client.host if request.client else None)
    return {"ok": True}


# ── Current user's workspace context ──────────────────────────────────────────
@router.get("/api/workspaces/my/list")
def api_my_workspaces(request: Request):
    """Return workspaces for the authenticated user.

    Global owner and API tokens receive ALL workspaces so they can always
    switch to any workspace, even if the workspace_members row is missing.
    Regular users receive only the workspaces they belong to.
    """
    user = _check_admin(request)
    uid = user.get("id", 0)
    if uid == 0 or user.get("role") == "owner":
        # Owner / API token — full list so switching is never blocked
        return list_workspaces()
    return list_user_workspaces(uid)


# ── Workspace switch (sets browser cookie) ────────────────────────────────────
@router.post("/api/workspaces/{workspace_id}/switch")
def api_switch_workspace(workspace_id: int, request: Request):
    """Set the active workspace cookie for the browser session.

    Validates that the user is a member of the workspace (or is the global
    owner), then sets the hr_workspace cookie which _resolve_workspace() reads.
    Returns the workspace row so the frontend can update its state immediately.
    """
    from fastapi.responses import JSONResponse
    from fastapi.encoders import jsonable_encoder
    from app.deps import WORKSPACE_COOKIE

    user = _check_admin(request)
    ws = get_workspace(workspace_id)
    if not ws:
        raise HTTPException(404, "Workspace not found")

    uid = user.get("id", 0)
    if user.get("role") != "owner" and uid != 0:
        member = get_workspace_member(workspace_id, uid)
        if not member:
            raise HTTPException(403, "You are not a member of this workspace")

    resp = JSONResponse(jsonable_encoder({"ok": True, "workspace": ws}))
    resp.set_cookie(
        WORKSPACE_COOKIE,
        str(workspace_id),
        httponly=False,   # JS needs to read this for display
        samesite="lax",
        max_age=365 * 86400,
    )
    return resp
