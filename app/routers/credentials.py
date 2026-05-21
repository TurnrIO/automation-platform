"""Credentials router."""
from typing import Optional
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.deps import _check_admin, _resolve_workspace
from app.core.db import list_credentials, upsert_credential, update_credential, delete_credential

router = APIRouter()


class CredCreate(BaseModel):
    name: str; type: str = "generic"; secret: str; note: str = ""


class CredUpdate(BaseModel):
    type: str = "generic"
    secret: Optional[str] = ""
    note: str = ""


@router.get("/api/credentials")
def api_creds(request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    return list_credentials(workspace_id=workspace_id)


@router.post("/api/credentials")
def api_cred_create(body: CredCreate, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    return upsert_credential(body.name, body.type, body.secret, body.note, workspace_id=workspace_id)


@router.put("/api/credentials/{cred_id}")
def api_cred_update(cred_id: int, body: CredUpdate, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    result = update_credential(cred_id, body.type, body.secret or "", body.note, workspace_id=workspace_id)
    if result is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Credential not found in workspace")
    return result


@router.delete("/api/credentials/{cred_id}")
def api_cred_delete(cred_id: int, request: Request):
    user = _check_admin(request)
    workspace_id = _resolve_workspace(request, user)
    deleted = delete_credential(cred_id, workspace_id=workspace_id)
    if not deleted:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Credential not found in workspace")
    return {"deleted": True}
