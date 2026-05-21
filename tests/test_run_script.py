"""Unit tests for action.run_script — feature flag and audit logging."""
import logging
import pytest


def _run(script, inp=None):
    from app.nodes.action_run_script import run
    logger = logging.getLogger("audit")
    return run({"script": script}, inp or {}, {}, logger)


class TestFeatureFlag:
    def test_disabled_when_env_absent(self, monkeypatch):
        monkeypatch.delenv("ENABLE_RUN_SCRIPT", raising=False)
        with pytest.raises(RuntimeError, match="disabled"):
            _run("result = 1")

    def test_disabled_when_set_to_false(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "false")
        with pytest.raises(RuntimeError, match="disabled"):
            _run("result = 1")

    def test_disabled_when_set_to_zero(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "0")
        with pytest.raises(RuntimeError, match="disabled"):
            _run("result = 1")

    def test_enabled_when_set_to_true(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "true")
        assert _run("result = {'x': 42}") == {"x": 42}

    def test_enabled_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "TRUE")
        assert _run("result = 'hello'") == "hello"

    def test_enabled_with_leading_whitespace(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "  true  ")
        assert _run("result = 1") == 1


class TestExecution:
    @pytest.fixture(autouse=True)
    def enable(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "true")

    def test_result_variable_returned(self):
        assert _run("result = {'answer': 42}") == {"answer": 42}

    def test_input_available_in_namespace(self):
        assert _run("result = input['v'] * 2", inp={"v": 5}) == 10

    def test_no_result_returns_input(self):
        inp = {"key": "value"}
        assert _run("x = 1", inp=inp) == inp

    def test_multiline_script(self):
        script = "nums = [1, 2, 3]\ntotal = sum(nums)\nresult = {'total': total}"
        assert _run(script) == {"total": 6}

    def test_script_error_raises_runtime_error(self):
        with pytest.raises(RuntimeError):
            _run("result = 1 / 0")


class TestAuditLogging:
    def test_audit_logger_called(self, monkeypatch, caplog):
        monkeypatch.setenv("ENABLE_RUN_SCRIPT", "true")
        import logging
        with caplog.at_level(logging.WARNING, logger="audit"):
            _run("result = 'logged'")
        audit_msgs = [r.message for r in caplog.records if r.name == "audit"]
        assert len(audit_msgs) >= 2, "Expected at least two audit log entries"
        assert any("hash=" in m for m in audit_msgs)

    def test_audit_log_before_disabled_raises(self, monkeypatch, caplog):
        """Audit log should NOT fire if the feature flag prevents execution."""
        monkeypatch.delenv("ENABLE_RUN_SCRIPT", raising=False)
        import logging
        with caplog.at_level(logging.WARNING, logger="audit"):
            with pytest.raises(RuntimeError):
                _run("result = 1")
        audit_msgs = [r for r in caplog.records if r.name == "audit"]
        assert audit_msgs == [], "No audit log should fire when disabled"
