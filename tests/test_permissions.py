"""Tests for the permission / auth guard layer (app/deps.py).

All tests run in-process without a live database.  DB helpers and the
session-cookie lookup are monkey-patched so we can exercise every code
path in _check_admin, _require_scope, _require_writer, _require_owner,
and _check_flow_access cleanly.

Note: _check_admin lazily imports get_current_user from app.auth inside
the function body, so we patch app.auth.get_current_user, not app.deps.get_current_user.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from starlette.requests import Request


# ── helpers ────────────────────────────────────────────────────────────────────

def _mock_request(headers: dict = None, cookies: dict = None) -> Request:
    """Build a minimal Starlette Request with the given headers/cookies."""
    raw_headers = []
    for k, v in (headers or {}).items():
        raw_headers.append((k.lower().encode(), v.encode()))
    if cookies:
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        raw_headers.append((b"cookie", cookie_str.encode()))

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": raw_headers,
    }
    return Request(scope)


def _owner() -> dict:
    return {"id": 1, "username": "owner", "role": "owner"}


def _admin() -> dict:
    return {"id": 2, "username": "admin", "role": "admin"}


def _viewer() -> dict:
    return {"id": 3, "username": "viewer", "role": "viewer"}


# _check_admin imports get_current_user + hash_token locally from app.auth,
# but get_api_token_by_hash and touch_api_token are bound at module import time in app.deps.
_GCU   = "app.auth.get_current_user"   # local import inside _check_admin
_ATB   = "app.auth.hash_token"          # local import inside _check_admin
_TOKDB = "app.deps.get_api_token_by_hash"  # module-level import in deps.py
_TOUCH = "app.deps.touch_api_token"        # module-level import in deps.py


# ── _check_admin ─────────────────────────────────────────────────────────────

class TestCheckAdmin:
    """_check_admin should resolve session users and token users."""

    def test_session_user_returned_directly(self):
        from app.deps import _check_admin
        req = _mock_request()
        with patch(_GCU, return_value=_owner()):
            user = _check_admin(req)
        assert user["role"] == "owner"

    def test_bearer_token_resolves(self):
        from app.deps import _check_admin
        req = _mock_request(headers={"Authorization": "Bearer tok123"})
        fake_token = {"name": "ci", "scope": "manage"}
        with patch(_GCU, return_value=None), \
             patch(_ATB, return_value="hashed"), \
             patch(_TOKDB, return_value=fake_token), \
             patch(_TOUCH):
            user = _check_admin(req)
        assert user["username"] == "api:ci"
        assert user["role"] == "owner"
        assert user["token_scope"] == "manage"

    def test_legacy_x_api_token_header(self):
        from app.deps import _check_admin
        req = _mock_request(headers={"x-api-token": "legacytok"})
        fake_token = {"name": "legacy", "scope": "read"}
        with patch(_GCU, return_value=None), \
             patch(_ATB, return_value="hashed"), \
             patch(_TOKDB, return_value=fake_token), \
             patch(_TOUCH):
            user = _check_admin(req)
        assert user["token_scope"] == "read"

    def test_no_credentials_raises_401(self):
        from app.deps import _check_admin
        req = _mock_request()
        with patch(_GCU, return_value=None), patch(_TOKDB, return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                _check_admin(req)
        assert exc_info.value.status_code == 401

    def test_invalid_token_raises_401(self):
        from app.deps import _check_admin
        req = _mock_request(headers={"Authorization": "Bearer badtok"})
        with patch(_GCU, return_value=None), \
             patch(_ATB, return_value="hashed2"), \
             patch(_TOKDB, return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                _check_admin(req)
        assert exc_info.value.status_code == 401


# ── _require_scope ────────────────────────────────────────────────────────────

class TestRequireScope:
    """_require_scope enforces token scope against the requested action."""

    def test_manage_token_passes_manage(self):
        from app.deps import _require_scope
        token_user = {"role": "owner", "token_scope": "manage"}
        # should not raise
        _require_scope(token_user, "manage")

    def test_run_token_passes_run(self):
        from app.deps import _require_scope
        token_user = {"role": "owner", "token_scope": "run"}
        _require_scope(token_user, "run")

    def test_read_token_blocked_by_run_scope(self):
        from app.deps import _require_scope
        token_user = {"role": "owner", "token_scope": "read"}
        with pytest.raises(HTTPException) as exc_info:
            _require_scope(token_user, "run")
        assert exc_info.value.status_code == 403

    def test_read_token_blocked_by_manage_scope(self):
        from app.deps import _require_scope
        token_user = {"role": "owner", "token_scope": "read"}
        with pytest.raises(HTTPException) as exc_info:
            _require_scope(token_user, "manage")
        assert exc_info.value.status_code == 403

    def test_session_user_bypasses_scope(self):
        from app.deps import _require_scope
        session_user = {"role": "owner"}   # no token_scope key
        # should not raise even with a high-privilege action
        _require_scope(session_user, "manage")

    def test_owner_passes_writer(self):
        from app.deps import _require_scope
        _require_scope(_owner(), "write")

    def test_admin_passes_writer(self):
        from app.deps import _require_scope
        _require_scope(_admin(), "write")

    def test_viewer_blocked_by_writer(self):
        from app.deps import _require_writer
        with pytest.raises(HTTPException) as exc_info:
            _require_writer(_viewer())
        assert exc_info.value.status_code == 403

    def test_owner_passes_owner_guard(self):
        from app.deps import _require_owner
        _require_owner(_owner())

    def test_admin_blocked_by_owner_guard(self):
        from app.deps import _require_owner
        with pytest.raises(HTTPException) as exc_info:
            _require_owner(_admin())
        assert exc_info.value.status_code == 403

    def test_viewer_blocked_by_owner_guard(self):
        from app.deps import _require_owner
        with pytest.raises(HTTPException) as exc_info:
            _require_owner(_viewer())
        assert exc_info.value.status_code == 403


# ── _require_writer ───────────────────────────────────────────────────────────

class TestRoleGuards:
    """Role-based guards (_require_owner, _require_admin) gate high-privilege operations."""

    def test_owner_always_granted(self):
        from app.deps import _require_owner
        _require_owner(_owner())

    def test_admin_always_granted(self):
        from app.deps import _require_admin
        _require_admin(_admin())

    def test_viewer_global_role_blocked_by_require_writer(self):
        """_require_writer checks global role only, not workspace_membership.

        Since _require_writer uses ROLE_LEVELS (viewer=0, admin=1, owner=2), a user
        with global role 'editor' (not in ROLE_LEVELS, treated as viewer=0) is blocked.
        A viewer with workspace_membership.role='editor' is also blocked — workspace_membership
        is not consulted by _require_writer.


        Use _check_flow_access for per-workspace permission checks.
        """
        from app.deps import _require_writer
        # Global role 'editor' is not in ROLE_LEVELS — treated as viewer, gets 0, blocked
        editor = {"id": 3, "username": "editor", "role": "editor"}
        with pytest.raises(HTTPException) as exc_info:
            _require_writer(editor)
        assert exc_info.value.status_code == 403

    def test_viewer_with_viewer_permission_blocked_for_runner(self):
        from app.deps import _require_writer
        viewer = {"id": 3, "username": "viewer", "role": "viewer", "workspace_membership": {"role": "viewer"}}
        with pytest.raises(HTTPException) as exc_info:
            _require_writer(viewer)
        assert exc_info.value.status_code == 403

    def test_viewer_with_no_permission_row_blocked(self):
        from app.deps import _require_writer
        viewer = {"id": 3, "username": "viewer", "role": "viewer", "workspace_membership": None}
        with pytest.raises(HTTPException) as exc_info:
            _require_writer(viewer)
        assert exc_info.value.status_code == 403

    def test_api_token_user_bypasses_flow_check(self):
        """API tokens should bypass workspace membership checks."""
        from app.deps import _require_writer
        token_user = {"id": 99, "role": "owner", "token_scope": "manage"}
        # should not raise — tokens bypass workspace membership
        _require_writer(token_user)


# ── _check_flow_access ─────────────────────────────────────────────────────────

class TestCheckFlowAccess:
    """_check_flow_access enforces run-time flow access for tokens."""

    def test_owner_allowed(self):
        from app.deps import _check_flow_access
        owner = {"id": 1, "role": "owner"}
        _check_flow_access(owner, "flow-123", "run")

    def test_admin_allowed(self):
        from app.deps import _check_flow_access
        admin = {"id": 2, "role": "admin"}
        _check_flow_access(admin, "flow-123", "run")

    def test_member_with_permission_allowed(self):
        from app.deps import _check_flow_access
        member = {"id": 5, "role": "member", "workspace_id": 1}
        with patch("app.deps.get_flow_permission", return_value={"role": "editor"}):
            _check_flow_access(member, 123, "runner")

    def test_member_without_permission_blocked(self):
        from app.deps import _check_flow_access
        member = {"id": 5, "role": "member", "workspace_id": 1}
        with patch("app.deps.get_flow_permission", return_value={"role": "viewer"}):
            with pytest.raises(HTTPException) as exc_info:
                _check_flow_access(member, 123, "runner")
        assert exc_info.value.status_code == 403

    def test_token_without_flow_access_blocked(self):
        """Token with insufficient flow permissions should be blocked."""
        from app.deps import _check_flow_access
        token = {"id": 99, "role": "member", "token_scope": "read"}
        with patch("app.deps.get_flow_permission", return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                _check_flow_access(token, 123, "run")
        assert exc_info.value.status_code == 403


# ── _resolve_workspace ─────────────────────────────────────────────────────────

class TestResolveWorkspace:
    """_resolve_workspace picks the right workspace from request headers / cookies."""

    def test_header_takes_priority(self):
        from app.deps import _resolve_workspace
        req = _mock_request(headers={"X-Workspace-Id": "42"})
        with patch("app.deps.get_workspace", return_value={"id": 42}):
            result = _resolve_workspace(req, _owner())
        assert result == 42

    def test_cookie_used_when_no_header(self):
        from app.deps import _resolve_workspace
        req = _mock_request(cookies={"hr_workspace": "7"})
        with patch("app.deps.get_workspace", return_value={"id": 7}):
            result = _resolve_workspace(req, _owner())
        assert result == 7

    def test_falls_back_to_default_workspace(self):
        from app.deps import _resolve_workspace
        req = _mock_request()
        with patch("app.deps.get_default_workspace", return_value={"id": 1}):
            result = _resolve_workspace(req, _viewer())
        assert result == 1

    def test_owner_bypasses_membership_check(self):
        from app.deps import _resolve_workspace
        req = _mock_request(headers={"X-Workspace-Id": "99"})
        with patch("app.deps.get_workspace", return_value={"id": 99}):
            result = _resolve_workspace(req, _owner())
        assert result == 99


# ── credentials router workspace isolation ─────────────────────────────────────

class TestCredentialIsolation:
    """Credential list/update/delete must be scoped to the caller's workspace.

    A token from workspace A must not be able to list, update, or delete
    credentials belonging to workspace B.
    """

    def _mock_cred_request(self, workspace_header: str = None):
        headers = {}
        if workspace_header:
            headers["X-Workspace-Id"] = workspace_header
        req = _mock_request(headers=headers)
        return req

    def test_list_credentials_requires_workspace_match(self):
        """Listing credentials should only return credentials for the caller's workspace."""
        from app.routers.credentials import api_creds
        # User from workspace 1 calling with X-Workspace-Id: 1
        req = self._mock_cred_request("1")
        user = {"id": 1, "role": "owner", "username": "owner", "token_scope": "manage"}
        with patch("app.auth.get_current_user", return_value=user), \
             patch("app.routers.credentials._resolve_workspace", return_value=1), \
             patch("app.routers.credentials.list_credentials") as mock_list:
            mock_list.return_value = [
                {"id": 1, "name": "cred-a", "workspace_id": 1},
                {"id": 2, "name": "cred-b", "workspace_id": 1},
            ]
            result = api_creds(req)
            # The router passes workspace_id to list_credentials, so it gets filtered
            mock_list.assert_called_once_with(workspace_id=1)
            assert all(r["workspace_id"] == 1 for r in result)

    def test_update_credential_requires_workspace_match(self):
        """Updating a credential from a different workspace should be rejected."""
        from app.routers.credentials import api_cred_update, CredUpdate
        from fastapi import HTTPException
        req = self._mock_cred_request("1")
        user = {"id": 1, "role": "owner", "username": "owner", "token_scope": "manage"}
        with patch("app.auth.get_current_user", return_value=user), \
             patch("app.deps._resolve_workspace", return_value=1), \
             patch("app.routers.credentials.update_credential", return_value=None):
            # update_credential returns None when the cred exists in another workspace but not this one
            with pytest.raises(HTTPException) as exc_info:
                api_cred_update(99, CredUpdate(type="webhook", secret="x"), req)
            assert exc_info.value.status_code == 404

    def test_delete_credential_requires_workspace_match(self):
        """Deleting a credential from a different workspace should be rejected."""
        from app.routers.credentials import api_cred_delete
        from fastapi import HTTPException
        req = self._mock_cred_request("1")
        user = {"id": 1, "role": "owner", "username": "owner", "token_scope": "manage"}
        with patch("app.auth.get_current_user", return_value=user), \
             patch("app.routers.credentials._resolve_workspace", return_value=1), \
             patch("app.routers.credentials.delete_credential", return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                api_cred_delete(99, req)
            assert exc_info.value.status_code == 404

    def test_cross_workspace_credential_update_blocked(self):
        """A token scoped to workspace 1 cannot update a credential belonging to workspace 2."""
        from app.routers.credentials import api_cred_update, CredUpdate
        from fastapi import HTTPException
        req = self._mock_cred_request("1")
        user = {"id": 1, "role": "owner", "username": "owner", "token_scope": "manage"}
        # update_credential finds the credential but it's in workspace 2
        with patch("app.auth.get_current_user", return_value=user), \
             patch("app.routers.credentials._resolve_workspace", return_value=1), \
             patch("app.routers.credentials.update_credential", return_value=None):
            # None means "not found in this workspace" → 404
            with pytest.raises(HTTPException) as exc_info:
                api_cred_update(55, CredUpdate(type="webhook", secret="x"), req)
            assert exc_info.value.status_code == 404
