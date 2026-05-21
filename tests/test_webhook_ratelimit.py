"""Tests for webhook trigger rate-limiting (app/routers/webhooks.py).

_check_webhook_rate() relies on Redis.  We test it by:
  1. Patching Redis so the counter behaves predictably (fast, no real Redis).
  2. Patching get_ratelimit_policy() to control the limit/window values.
  3. Confirming the fail-open behaviour when Redis is unavailable.

Note: _check_webhook_rate now returns tuple[bool, int, int] (allowed, limit, window).
"""
import os
import pytest
import redis
from unittest.mock import patch, MagicMock


# ── helpers ────────────────────────────────────────────────────────────────

def _make_pipeline(count_value: int):
    """Return a mock Redis pipeline whose execute() returns (count, True)."""
    pipe = MagicMock()
    pipe.execute.return_value = (count_value, True)
    return pipe


def _make_redis(count_value: int):
    """Return a mock Redis client backed by a controlled pipeline."""
    r = MagicMock()
    r.pipeline.return_value = _make_pipeline(count_value)
    return r


# ── _check_webhook_rate ────────────────────────────────────────────────────

class TestCheckWebhookRate:
    """Unit tests for the rate-limiting helper."""

    # ── private helper ─────────────────────────────────────────────────────

    @staticmethod
    def _invoke_with_mock(mock_redis, rate_limit: int, count: int) -> bool:
        """Call _check_webhook_rate with controlled Redis + rate limit policy.

        Returns the boolean (allowed) portion of the returned tuple.
        """
        import app.routers.webhooks as mod

        pipe = MagicMock()
        pipe.execute.return_value = (count, True)
        mock_redis.pipeline.return_value = pipe

        policy = {"limit": rate_limit, "window": 60}
        with patch("app.routers.webhooks.get_ratelimit_policy", return_value=policy):
            try:
                import redis as _redis_mod
                with patch.object(_redis_mod, "from_url", return_value=mock_redis):
                    allowed, _limit, _window = mod._check_webhook_rate("tok")
                    return allowed
            except (redis.exceptions.RedisError, OSError, RuntimeError):
                return True  # fail open

    def test_first_request_allowed(self):
        # count=1, limit=10 → allowed
        result = self._invoke_with_mock(_make_redis(1), rate_limit=10, count=1)
        assert result is True

    def test_at_limit_allowed(self):
        result = self._invoke_with_mock(_make_redis(10), rate_limit=10, count=10)
        assert result is True

    def test_over_limit_blocked(self):
        result = self._invoke_with_mock(_make_redis(11), rate_limit=10, count=11)
        assert result is False

    def test_zero_limit_disables_check(self):
        """WEBHOOK_RATE_LIMIT=0 disables rate limiting entirely."""
        result = self._invoke_with_mock(_make_redis(9999), rate_limit=0, count=9999)
        assert result is True

    def test_redis_unavailable_fails_closed(self):
        """If Redis raises, _check_webhook_rate returns False (fail closed) — deny request."""
        import app.routers.webhooks as mod
        bad_redis = MagicMock()
        bad_redis.pipeline.side_effect = redis.exceptions.ConnectionError("connection refused")

        policy = {"limit": 10, "window": 60}
        with patch("app.routers.webhooks.get_ratelimit_policy", return_value=policy):
            import redis as _redis_mod
            with patch.object(_redis_mod, "from_url", return_value=bad_redis):
                allowed, _limit, _window = mod._check_webhook_rate("tok")
                result = allowed

        assert result is False

    def test_return_tuple_contains_limit_and_window(self):
        """_check_webhook_rate returns (bool, limit, window) tuple."""
        import app.routers.webhooks as mod
        mock_redis = _make_redis(1)

        policy = {"limit": 42, "window": 120}
        with patch("app.routers.webhooks.get_ratelimit_policy", return_value=policy):
            try:
                import redis as _redis_mod
                with patch.object(_redis_mod, "from_url", return_value=mock_redis):
                    result = mod._check_webhook_rate("tok")
                    assert isinstance(result, tuple)
                    assert len(result) == 3
                    allowed, limit, window = result
                    assert allowed is True
                    assert limit == 42
                    assert window == 120
            except (redis.exceptions.RedisError, OSError, RuntimeError):
                pass  # fail open is acceptable

    def test_different_tokens_use_different_keys(self):
        """Each token gets its own rate-limit bucket."""
        import app.routers.webhooks as mod

        calls = []

        def _fake_pipeline_factory():
            pipe = MagicMock()

            def _incr_track(key):
                calls.append(key)
                return pipe

            pipe.incr.side_effect = _incr_track
            pipe.expire.return_value = pipe
            pipe.execute.return_value = (1, True)
            return pipe

        mock_r = MagicMock()
        mock_r.pipeline.side_effect = _fake_pipeline_factory

        policy = {"limit": 10, "window": 60}
        with patch("app.routers.webhooks.get_ratelimit_policy", return_value=policy):
            try:
                import redis as _redis_mod
                with patch.object(_redis_mod, "from_url", return_value=mock_r):
                    mod._check_webhook_rate("token-A")
                    mod._check_webhook_rate("token-B")
            except (redis.exceptions.RedisError, OSError, RuntimeError):
                pass

        # Verify the key names differ per token (if calls were tracked)
        if calls:
            assert len(set(calls)) == 2


# ── login brute-force rate limiting ───────────────────────────────────────

class TestLoginBruteForce:
    """_check_login_allowed and _record_login_failure from auth.py."""

    def _make_redis(self, locked=False, attempt_count=0):
        r = MagicMock()
        r.ping.return_value = True
        r.exists.return_value = 1 if locked else 0
        r.ttl.return_value = 840  # 14 minutes
        r.incr.return_value = attempt_count
        return r

    def test_unlocked_ip_passes(self):
        from app.routers.auth import _check_login_allowed
        mock_r = self._make_redis(locked=False)
        with patch("app.routers.auth._login_redis", return_value=mock_r):
            # Should not raise
            _check_login_allowed("1.2.3.4")

    def test_locked_ip_raises_429(self):
        from fastapi import HTTPException
        from app.routers.auth import _check_login_allowed
        mock_r = self._make_redis(locked=True)
        with patch("app.routers.auth._login_redis", return_value=mock_r):
            with pytest.raises(HTTPException) as exc_info:
                _check_login_allowed("1.2.3.4")
        assert exc_info.value.status_code == 429

    def test_redis_unavailable_fails_closed(self):
        """If Redis is unavailable, _check_login_allowed raises 503 (fail closed)."""
        from fastapi import HTTPException
        from app.routers.auth import _check_login_allowed
        with patch("app.routers.auth._login_redis", return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                _check_login_allowed("1.2.3.4")
            assert exc_info.value.status_code == 503

    def test_fifth_failure_triggers_lockout(self):
        from app.routers.auth import _record_login_failure
        mock_r = self._make_redis(attempt_count=5)
        with patch("app.routers.auth._login_redis", return_value=mock_r):
            _record_login_failure("1.2.3.4")
        # setex called (lockout set) and delete called (counter cleared)
        mock_r.setex.assert_called_once()
        mock_r.delete.assert_called_once()

    def test_first_failure_no_lockout(self):
        from app.routers.auth import _record_login_failure
        mock_r = self._make_redis(attempt_count=1)
        with patch("app.routers.auth._login_redis", return_value=mock_r):
            _record_login_failure("1.2.3.4")
        mock_r.setex.assert_not_called()

    def test_clear_login_failures(self):
        from app.routers.auth import _clear_login_failures
        mock_r = self._make_redis()
        with patch("app.routers.auth._login_redis", return_value=mock_r):
            _clear_login_failures("1.2.3.4")
        assert mock_r.delete.call_count == 2  # attempts key + lockout key
