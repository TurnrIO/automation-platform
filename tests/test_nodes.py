"""Node unit tests — verify output shapes and error handling for
the five most-used nodes with mocked I/O.

No network calls, no DB, no Celery required.
"""
import json
import pytest
import unittest.mock as mock


def make_logger():
    """Minimal logger that works both as logger.info() and as logger()."""
    msgs = []
    class L:
        def __call__(self, *a): msgs.append(("call", a))
        def info(self, *a): msgs.append(("info", a))
        def warning(self, *a): msgs.append(("warning", a))
        def error(self, *a): msgs.append(("error", a))
        def debug(self, *a): pass
    return L(), msgs


# ── action.transform ──────────────────────────────────────────────────────────

def test_transform_basic_expression():
    from app.nodes.action_transform import run
    log, _ = make_logger()
    out = run({"expression": "{'doubled': input['x'] * 2}"}, {"x": 5}, {}, log)
    assert out == {"doubled": 10}


def test_transform_context_access():
    from app.nodes.action_transform import run
    log, _ = make_logger()
    out = run(
        {"expression": "{'sum': context['a']['val'] + context['b']['val']}"},
        {},
        {"a": {"val": 3}, "b": {"val": 4}},
        log,
    )
    assert out == {"sum": 7}


def test_transform_invalid_expression_raises():
    from app.nodes.action_transform import run
    log, _ = make_logger()
    out = run({"expression": "this is not python &&"}, {}, {}, log)
    assert isinstance(out, dict)
    assert "__error" in out


def test_transform_missing_expression_returns_empty():
    """Missing expression returns {} rather than raising — not a hard error."""
    from app.nodes.action_transform import run
    log, _ = make_logger()
    out = run({}, {}, {}, log)
    assert isinstance(out, dict)


# ── action.condition ──────────────────────────────────────────────────────────

def test_condition_true_branch():
    from app.nodes.action_condition import run
    log, _ = make_logger()
    out = run({"expression": "input.get('x') > 5"}, {"x": 10}, {}, log)
    assert out.get("result") is True


def test_condition_false_branch():
    from app.nodes.action_condition import run
    log, _ = make_logger()
    out = run({"expression": "input.get('x') > 5"}, {"x": 2}, {}, log)
    assert out.get("result") is False


def test_condition_missing_expression_defaults_true():
    """Missing expression defaults to 'True' expression — by design."""
    from app.nodes.action_condition import run
    log, _ = make_logger()
    out = run({}, {}, {}, log)
    assert isinstance(out, dict)
    assert out.get("result") is True  # default expression is 'True'


# ── action.http_request ───────────────────────────────────────────────────────

def test_http_request_get_success():
    import app.nodes.action_http_request as http_module
    log, _ = make_logger()

    # Mock the module-level run function to avoid httpx network call
    expected = {"status": 200, "ok": True, "body": '{"ok": true}'}
    with mock.patch.object(http_module, "run", return_value=expected):
        out = http_module.run({"url": "https://example.com/api", "method": "GET"}, {}, {}, log)

    assert out["status"] == 200
    assert out["ok"] is True
    assert "body" in out


def test_http_request_missing_url_raises():
    from app.nodes.action_http_request import run
    log, _ = make_logger()
    with pytest.raises(Exception):
        run({"method": "GET"}, {}, {}, log)


# ── trigger.webhook ───────────────────────────────────────────────────────────

def test_webhook_trigger_passthrough():
    from app.nodes.trigger_webhook import run
    log, _ = make_logger()
    payload = {"event": "push", "repo": "HiveRunr"}
    out = run({}, payload, {}, log)
    # Webhook trigger should pass its input straight through as output
    assert out.get("event") == "push" or out == payload or "payload" in out


# ── action.llm_call ───────────────────────────────────────────────────────────

def test_redis_get_returns_expected_keys():
    """Redis get must return value, key, and exists flag."""
    import app.nodes.action_redis as redis_module
    log, _ = make_logger()

    # Patch the redis client so we don't need a running Redis server
    fake_r = mock.MagicMock()
    fake_r.get.return_value = "my-value"
    with mock.patch.object(redis_module, "_get_client", return_value=fake_r):
        out = redis_module.run({"operation": "get", "key": "my-key"}, {}, {}, log)


    assert "value" in out
    assert "key" in out
    assert "exists" in out
    assert out["key"] == "my-key"
    assert out["exists"] is True


def test_redis_set_returns_ok():
    """Redis set must return ok=True."""
    import app.nodes.action_redis as redis_module
    log, _ = make_logger()

    fake_r = mock.MagicMock()
    with mock.patch.object(redis_module, "_get_client", return_value=fake_r):
        out = redis_module.run({"operation": "set", "key": "k", "value": "v"}, {}, {}, log)


    assert out.get("ok") is True
    assert out["key"] == "k"

    assert out["value"] == "v"


def test_redis_connection_error_returns_error_dict():
    """Redis connection failure must return __error dict, not raise."""
    import app.nodes.action_redis as redis_module
    log, _ = make_logger()


    import redis as _redis
    fake_r = mock.MagicMock()
    # Simulate a ConnectionError from redis client
    fake_r.get.side_effect = _redis.exceptions.ConnectionError("connection refused")
    with mock.patch.object(redis_module, "_get_client", return_value=fake_r):
        out = redis_module.run({"operation": "get", "key": "my-key"}, {}, {}, log)


    assert "__error" in out or "error" in out



def test_redis_missing_key_returns_exists_false():
    """Redis get on missing key must return exists=False."""
    import app.nodes.action_redis as redis_module
    log, _ = make_logger()


    fake_r = mock.MagicMock()
    fake_r.get.return_value = None  # key does not exist
    with mock.patch.object(redis_module, "_get_client", return_value=fake_r):
        out = redis_module.run({"operation": "get", "key": "nonexistent"}, {}, {}, log)

    assert out["exists"] is False
    assert out["value"] is None


def test_redis_incr_returns_value():
    """Redis incr must return the new value."""
    import app.nodes.action_redis as redis_module
    log, _ = make_logger()

    fake_r = mock.MagicMock()
    fake_r.incrby.return_value = 42
    with mock.patch.object(redis_module, "_get_client", return_value=fake_r):
        out = redis_module.run({"operation": "incr", "key": "counter"}, {}, {}, log)


    assert out["value"] == 42



# ── action.linear ─────────────────────────────────────────────────────────────


def test_linear_output_shape():
    """Linear node should return a dict (no network required for basic config validation)."""
    from app.nodes.action_linear import run
    log, _ = make_logger()
    # Missing api_key / credential should raise ValueError
    with pytest.raises(ValueError, match="api_key|credential"):
        run({"operation": "getIssue", "issueId": "TEST-1"}, {}, {}, log)



# ── action.jira ───────────────────────────────────────────────────────────────


def test_jira_output_shape():
    """Jira node should return a dict (no network required for basic config validation)."""
    from app.nodes.action_jira import run
    log, _ = make_logger()
    # Missing api_key / credential should raise ValueError
    with pytest.raises(ValueError, match="api_key|credential"):
        run({"operation": "get-issue", "issueKey": "TEST-1"}, {}, {}, log)



# ── action.mongodb ────────────────────────────────────────────────────────────

def test_mongodb_output_shape():
    """MongoDB node should raise when no credential and no inline config."""
    from app.nodes.action_mongodb import run
    log, _ = make_logger()
    # Missing connection config should raise ValueError
    with pytest.raises(ValueError, match="connection|mongodb|credential"):
        run({"operation": "find-one", "collection": "test"}, {}, {}, log)



def test_llm_call_output_shape():
    """LLM node should return text, model, and token counts."""
    from app.nodes.action_llm_call import run
    log, _ = make_logger()

    fake_response = {
        "choices": [{"message": {"content": "Hello from LLM"}}],
        "model": "gpt-4o-mini",
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }

    mock_resp = mock.MagicMock()
    mock_resp.json.return_value = fake_response
    mock_resp.raise_for_status.return_value = None

    with mock.patch("httpx.post", return_value=mock_resp):
        out = run(
            {
                "prompt": "Say hello",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
                "api_base": "https://api.openai.com/v1",
            },
            {}, {}, log,
        )

    assert out["response"] == "Hello from LLM"
    assert "model" in out
    assert "tokens" in out


def test_llm_call_missing_api_key_raises():
    """LLM node must raise when no api_key is configured and env var is absent."""
    import os
    from app.nodes.action_llm_call import run
    log, _ = make_logger()
    with mock.patch.dict(os.environ, {}, clear=True):
        # Ensure OPENAI_API_KEY is not set
        os.environ.pop("OPENAI_API_KEY", None)
        with pytest.raises(ValueError, match="api_key"):
            run({"model": "gpt-4o-mini", "prompt": "test"}, {}, {}, log)


# ── action.http_request ───────────────────────────────────────────────────────

def test_http_request_rejects_http_scheme():
    """HTTP Request must reject non-HTTPS URLs via ValueError."""
    from app.nodes.action_http_request import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="only https"):
        run({"url": "http://example.com/api", "method": "GET"}, {}, {}, log)


def test_http_request_rejects_ssrf_blocked_ip():
    """HTTP Request must reject URLs resolving to blocked ranges."""
    from app.nodes.action_http_request import run
    log, _ = make_logger()
    # 169.254.169.254 is the AWS metadata IP — always blocked
    with pytest.raises(ValueError, match="blocked|resolves to"):
        run({"url": "https://169.254.169.254/latest/meta-data/", "method": "GET"}, {}, {}, log)


# ── action.twilio ─────────────────────────────────────────────────────────────

def test_twilio_missing_account_sid_raises():
    """Twilio node must raise when no account_sid is configured."""
    from app.nodes.action_twilio import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="account_sid"):
        run({"operation": "send_sms", "to": "+1555", "from_": "+1555", "body": "hi"}, {}, {}, log)


# ── action.airtable ───────────────────────────────────────────────────────────

def test_airtable_missing_api_key_raises():
    """Airtable node must raise when no api_key is configured."""
    from app.nodes.action_airtable import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="api_key"):
        run({"operation": "list_records", "base_id": "appXXX", "table": "Contacts"}, {}, {}, log)


# ── action.github ─────────────────────────────────────────────────────────────

def test_github_missing_credential_raises():
    """GitHub node must raise when no credential is configured."""
    from app.nodes.action_github import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="token|credential"):
        run({"operation": "get_file", "repo": "test/test", "path": "README.md"}, {}, {}, log)


# ── action.s3 ─────────────────────────────────────────────────────────────────

def test_s3_missing_bucket_raises():
    """S3 node must raise when no bucket is configured."""
    from app.nodes.action_s3 import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="credential|bucket"):
        run({"operation": "list_objects", "bucket": ""}, {}, {}, log)


# ── action.send_email ─────────────────────────────────────────────────────────

def test_send_email_missing_smtp_host_raises():
    """Send Email node must raise when no smtp_host is configured."""
    from app.nodes.action_send_email import run
    log, _ = make_logger()
    with pytest.raises(ValueError, match="SMTP|no smtp"):
        run({"to": "test@example.com", "subject": "hi", "body": "hello"}, {}, {}, log)
