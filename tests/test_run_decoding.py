import unittest.mock as mock

import psycopg2.extras


def test_decode_json_value_handles_string_and_fallback():
    from app.core.db import decode_json_value

    assert decode_json_value('{"ok": true}', {}) == {"ok": True}
    assert decode_json_value("[1,2]", []) == [1, 2]
    assert decode_json_value("not-json", {}) == {}
    assert decode_json_value(None, []) == []


def test_decode_run_row_normalizes_all_run_json_fields():
    from app.core.db import decode_run_row

    row = {
        "task_id": "abc",
        "traces": '[{"node_id":"n1","status":"ok"}]',
        "result": '{"error":"boom"}',
        "initial_payload": '{"hello":"world"}',
    }

    decoded = decode_run_row(row)
    assert decoded["traces"] == [{"node_id": "n1", "status": "ok"}]
    assert decoded["result"] == {"error": "boom"}
    assert decoded["initial_payload"] == {"hello": "world"}


def test_api_get_run_payload_uses_real_dict_cursor_and_decodes_payload():
    from app.routers import runs as runs_mod

    fake_cur = mock.MagicMock()
    fake_cur.fetchone.return_value = {"initial_payload": '{"x": 1}', "workspace_id": 1}
    fake_conn = mock.MagicMock()
    fake_conn.__enter__ = lambda s: s
    fake_conn.__exit__ = mock.MagicMock(return_value=False)
    fake_conn.cursor.return_value = fake_cur

    with mock.patch.object(runs_mod, "_require_run_scope", return_value={"id": 1}), \
         mock.patch("app.deps._resolve_workspace", return_value=1), \
         mock.patch("app.core.db.get_conn", return_value=fake_conn):
        result = runs_mod.api_get_run_payload(7, mock.MagicMock())

    assert result == {"run_id": 7, "payload": {"x": 1}}
    _, kwargs = fake_conn.cursor.call_args
    assert kwargs["cursor_factory"] is psycopg2.extras.RealDictCursor
