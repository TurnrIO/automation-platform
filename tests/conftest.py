"""
conftest.py — test fixtures for tests/

An autouse fixture patches the three DB-helper functions that tests
regularly redirect through, so that every test runs without a live
PostgreSQL instance.

Patch targets (app.routers.credentials.*) are chosen because
app.api_creds imports these functions at router-creation time —
patching the import site, not the definition site, is the only way
to intercept the reference held by the caller.
"""
import pytest
from unittest.mock import MagicMock


# Shared mock objects — all tests in this file share the same mocks so
# cross-test isolation is not a goal here (each test class is independent).
_mock_conn = MagicMock()
_mock_conn.__enter__ = MagicMock(return_value=MagicMock())
_mock_conn.__exit__ = MagicMock(return_value=False)

_cred_store = {
    1: [
        {"id": "cred-a",  "workspace_id": 1, "name": "Cred A"},
        {"id": "cred-b",  "workspace_id": 1, "name": "Cred B"},
    ]
}
_list_creds_store = _cred_store  # populated by individual tests


@pytest.fixture(autouse=True)
def _patch_db_helpers():
    """
    Redirect every code path that would open a real DB connection so
    that tests which call _resolve_workspace / get_workspace /
    list_credentials / get_workspace_member run fully in-memory.
    """
    from unittest.mock import patch

    # Prime the import path so patch() can find app.core.db.
    # On some Python/path environments the lazy importer resolves this
    # as a submodule only after an explicit import.
    import app.core.db  # noqa: F401

    def mock_get_conn():
        return _mock_conn

    def mock_get_workspace(workspace_id):
        workspaces = {
            1: {"id": 1, "name": "Workspace 1"},
            2: {"id": 2, "name": "Workspace 2"},
            None: {"id": 1, "name": "Default Workspace"},
        }
        return workspaces.get(workspace_id, {"id": 1, "name": "Default Workspace"})

    def mock_get_workspace_member(workspace_id, user_id):
        return {"workspace_id": workspace_id, "user_id": user_id, "role": "member"}

    with patch("app.core.db.get_conn", mock_get_conn), \
         patch("app.core.db.get_workspace", mock_get_workspace), \
         patch("app.core.db.get_workspace_member", mock_get_workspace_member), \
         patch("app.routers.credentials.list_credentials") as mock_list:
        def _list_creds(workspace_id):
            return _list_creds_store.get(workspace_id, [])
        mock_list.side_effect = _list_creds
        yield
