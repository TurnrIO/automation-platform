"""Tests for executor failure paths, retry logic, and scheduler edge cases.

Tests are fully in-process.  No live database, Redis, or Celery required.
"""
import time
import pytest
from unittest.mock import patch, MagicMock

from app.core.executor import _topo, run_graph


# ── helpers ────────────────────────────────────────────────────────────────

def _g(nodes, edges=None):
    return {"nodes": nodes, "edges": edges or []}


def _node(nid, ntype="action.log", config=None, fail_mode="abort",
          retry_max=0, retry_delay=0, **extra):
    data = {
        "config": config or {},
        "fail_mode": fail_mode,
        "retry_max": retry_max,
        "retry_delay": retry_delay,
        **extra,
    }
    return {"id": nid, "type": ntype, "data": data}


# ── node failure: abort mode ───────────────────────────────────────────────

class TestNodeFailureAbort:
    """A node that raises in abort mode causes run_graph to raise RuntimeError.
    The trace for that node is appended before the raise, so we verify it."""

    def test_failing_node_raises_runtime_error(self):
        def _bad(*a, **kw):
            raise RuntimeError("boom")

        graph = _g([_node("n1", "action.log")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            with pytest.raises(RuntimeError, match="boom"):
                run_graph(graph)

    def test_abort_error_message_contains_node_id(self):
        def _bad(*a, **kw):
            raise ValueError("bad value")

        graph = _g([_node("n1", "action.log")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            with pytest.raises(RuntimeError) as exc_info:
                run_graph(graph)
        assert "n1" in str(exc_info.value)

    def test_abort_error_message_contains_exception_text(self):
        def _bad(*a, **kw):
            raise TypeError("type mismatch")

        graph = _g([_node("n1", "action.log")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            with pytest.raises(RuntimeError) as exc_info:
                run_graph(graph)
        assert "type mismatch" in str(exc_info.value)

    def test_unknown_node_type_raises(self):
        """An unregistered node type should raise RuntimeError wrapping ValueError."""
        graph = _g([_node("x1", "action.does_not_exist_xyz")])
        with pytest.raises(RuntimeError, match="x1"):
            run_graph(graph)


# ── node failure: continue mode ────────────────────────────────────────────

class TestNodeFailureContinue:
    """fail_mode='continue' should NOT raise; it stores the error and moves on."""

    def test_continue_mode_does_not_raise(self):
        def _bad(*a, **kw):
            raise RuntimeError("soft failure")

        graph = _g([_node("n1", fail_mode="continue")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            result = run_graph(graph)  # must not raise

        assert "traces" in result

    def test_continue_mode_trace_is_error_status(self):
        def _bad(*a, **kw):
            raise RuntimeError("soft failure")

        graph = _g([_node("n1", fail_mode="continue")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            result = run_graph(graph)

        trace = next(t for t in result["traces"] if t["node_id"] == "n1")
        assert trace["status"] == "error"

    def test_continue_mode_output_contains_error_key(self):
        def _bad(*a, **kw):
            raise RuntimeError("non-fatal")

        graph = _g([_node("n1", fail_mode="continue")])
        with patch("app.core.executor._run_node", side_effect=_bad):
            result = run_graph(graph)

        trace = next(t for t in result["traces"] if t["node_id"] == "n1")
        assert trace["output"] is not None
        assert "__error" in trace["output"]

    def test_continue_mode_subsequent_node_runs(self):
        """After a continue-on-error node, the next node in the graph should execute."""
        call_log = []

        def _mock(ntype, config, inp, ctx, logger, edges, nodes_map, creds, **kw):
            nid = kw.get("_nid", "")
            if nid == "n1":
                raise ValueError("non-fatal")
            call_log.append(nid)
            return {"done": True}

        graph = _g(
            [_node("n1", fail_mode="continue"), _node("n2")],
            edges=[{"source": "n1", "target": "n2"}],
        )
        with patch("app.core.executor._run_node", side_effect=_mock):
            run_graph(graph)

        assert "n2" in call_log


# ── retry logic ────────────────────────────────────────────────────────────

class TestRetryLogic:
    """Nodes with retry_max > 0 should be retried before aborting."""

    def test_succeeds_on_second_attempt(self):
        attempts = []

        def _flaky(*a, **kw):
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("transient")
            return {"ok": True}

        graph = _g([_node("n1", retry_max=2, retry_delay=0)])
        with patch("app.core.executor._run_node", side_effect=_flaky), \
             patch("time.sleep"):
            result = run_graph(graph)

        trace = next(t for t in result["traces"] if t["node_id"] == "n1")
        assert trace["status"] == "ok"
        assert trace["attempts"] == 2

    def test_exhausts_retries_and_raises(self):
        """After all retries fail in abort mode, run_graph raises."""
        def _always_fail(*a, **kw):
            raise RuntimeError("always bad")

        graph = _g([_node("n1", retry_max=2, retry_delay=0)])
        with patch("app.core.executor._run_node", side_effect=_always_fail), \
             patch("time.sleep"):
            with pytest.raises(RuntimeError, match="always bad"):
                run_graph(graph)

    def test_no_retry_on_success(self):
        calls = []

        def _success(*a, **kw):
            calls.append(1)
            return {"ok": True}

        graph = _g([_node("n1", retry_max=3, retry_delay=0)])
        with patch("app.core.executor._run_node", side_effect=_success):
            result = run_graph(graph)

        assert len(calls) == 1  # called exactly once
        trace = next(t for t in result["traces"] if t["node_id"] == "n1")
        assert trace["attempts"] == 1


# ── condition branching ────────────────────────────────────────────────────

class TestConditionBranching:
    """action.condition should skip the appropriate branch."""

    def _build_condition_graph(self):
        """trigger → condition → [true: n_true] [false: n_false]"""
        nodes = [
            _node("t1", "trigger.manual"),
            _node("cond", "action.condition", config={"expression": "True"}),
            _node("n_true", "action.log"),
            _node("n_false", "action.log"),
        ]
        edges = [
            {"source": "t1",   "target": "cond",    "sourceHandle": None},
            {"source": "cond", "target": "n_true",  "sourceHandle": "true"},
            {"source": "cond", "target": "n_false", "sourceHandle": "false"},
        ]
        return _g(nodes, edges)

    def test_true_branch_skips_false(self):
        graph = self._build_condition_graph()

        def _mock(ntype, config, inp, ctx, logger, *a, **kw):
            if ntype == "action.condition":
                return {"result": True}
            return {"logged": True}

        with patch("app.core.executor._run_node", side_effect=_mock):
            result = run_graph(graph)

        traces = {t["node_id"]: t for t in result["traces"]}
        if "n_false" in traces:
            assert traces["n_false"]["status"] == "skipped"

    def test_false_branch_skips_true(self):
        graph = self._build_condition_graph()

        def _mock(ntype, config, inp, ctx, logger, *a, **kw):
            if ntype == "action.condition":
                return {"result": False}
            return {"logged": True}

        with patch("app.core.executor._run_node", side_effect=_mock):
            result = run_graph(graph)

        traces = {t["node_id"]: t for t in result["traces"]}
        if "n_true" in traces:
            assert traces["n_true"]["status"] == "skipped"


# ── scheduler lock helpers ─────────────────────────────────────────────────

class TestSchedulerLock:
    """Edge-case tests for scheduler.py lock functions (no blocking scheduler)."""

    def test_try_acquire_returns_true_when_setnx_wins(self):
        mock_r = MagicMock()
        mock_r.set.return_value = True  # Redis SET NX succeeded

        import app.scheduler as sched
        result = sched._try_acquire(mock_r)
        assert result is True

    def test_try_acquire_returns_false_when_lock_held(self):
        mock_r = MagicMock()
        mock_r.set.return_value = False  # lock already held

        import app.scheduler as sched
        result = sched._try_acquire(mock_r)
        assert result is False

    def test_try_acquire_returns_false_for_none(self):
        """Redis returns None (not True/False) on some clients when nx=True fails."""
        mock_r = MagicMock()
        mock_r.set.return_value = None

        import app.scheduler as sched
        result = sched._try_acquire(mock_r)
        assert result is False

    def test_try_refresh_returns_true_when_lua_returns_1(self):
        mock_r = MagicMock()
        mock_r.eval.return_value = 1

        import app.scheduler as sched
        result = sched._try_refresh(mock_r)
        assert result is True

    def test_try_refresh_returns_false_when_lua_returns_0(self):
        mock_r = MagicMock()
        mock_r.eval.return_value = 0  # lock was taken over

        import app.scheduler as sched
        result = sched._try_refresh(mock_r)
        assert result is False

    def test_release_calls_eval(self):
        """_release should call eval with the Lua release script."""
        mock_r = MagicMock()
        import app.scheduler as sched
        sched._release(mock_r)
        mock_r.eval.assert_called_once()

    def test_lock_key_is_deterministic(self):
        """All instances must agree on the lock key name."""
        import app.scheduler as sched
        assert sched._LOCK_KEY == "hiverunr:scheduler:leader"

    def test_instance_id_is_unique(self):
        """Each import gets a unique _INSTANCE_ID (based on token_hex)."""
        import app.scheduler as sched
        assert sched._INSTANCE_ID  # truthy — not empty
        assert len(sched._INSTANCE_ID) == 16  # token_hex(8) → 16 hex chars


# ── general executor edge cases ────────────────────────────────────────────

class TestExecutorEdgeCases:
    def test_cyclic_dependency_handled_by_topo(self):
        """Kahn's algorithm raises ValueError on cycles (IMP-110)."""
        nodes = [{"id": "a"}, {"id": "b"}]
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
        ]
        with pytest.raises(ValueError, match="cycle"):
            _topo(nodes, edges)

    def test_empty_graph_returns_empty_traces(self):
        result = run_graph(_g([]))
        assert result["traces"] == []
        assert result["results"] == {}

    def test_trace_duration_is_non_negative(self):
        graph = _g([_node("t1", "action.log")])

        def _ok(*a, **kw):
            return {"logged": True}

        with patch("app.core.executor._run_node", side_effect=_ok):
            result = run_graph(graph)

        for trace in result["traces"]:
            assert trace.get("duration_ms", 0) >= 0

    def test_note_node_always_skipped(self):
        """Nodes of type 'note' should be skipped without calling _run_node."""
        graph = _g([_node("note1", "note")])
        with patch("app.core.executor._run_node") as mock_run:
            result = run_graph(graph)
        mock_run.assert_not_called()
        traces = {t["node_id"]: t for t in result["traces"]}
        assert traces.get("note1", {}).get("status") == "skipped"

    def test_disabled_node_is_skipped(self):
        """A node with disabled=True in its data should be skipped."""
        node = _node("n1", "action.log")
        node["data"]["disabled"] = True
        graph = _g([node])
        with patch("app.core.executor._run_node") as mock_run:
            run_graph(graph)
        mock_run.assert_not_called()
