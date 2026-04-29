"""Tests for pipewatch.throttle — AlertThrottler and ThrottlePolicy."""

import pytest

from pipewatch.throttle import AlertThrottler, ThrottlePolicy


class TestThrottlePolicy:
    def test_default_cooldown(self):
        p = ThrottlePolicy()
        assert p.cooldown_seconds == 60.0

    def test_negative_cooldown_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            ThrottlePolicy(cooldown_seconds=-1)

    def test_zero_max_firings_raises(self):
        with pytest.raises(ValueError, match="max_firings"):
            ThrottlePolicy(max_firings=0)

    def test_valid_max_firings(self):
        p = ThrottlePolicy(max_firings=3)
        assert p.max_firings == 3


class TestAlertThrottler:
    def _throttler(self, cooldown=10.0, max_firings=None):
        return AlertThrottler(ThrottlePolicy(cooldown_seconds=cooldown, max_firings=max_firings))

    def test_first_firing_allowed(self):
        t = self._throttler()
        assert t.allow("pipeline_a", now=100.0) is True

    def test_second_firing_within_cooldown_blocked(self):
        t = self._throttler(cooldown=10.0)
        t.allow("pipeline_a", now=100.0)
        assert t.allow("pipeline_a", now=105.0) is False

    def test_firing_after_cooldown_allowed(self):
        t = self._throttler(cooldown=10.0)
        t.allow("pipeline_a", now=100.0)
        assert t.allow("pipeline_a", now=111.0) is True

    def test_different_keys_are_independent(self):
        t = self._throttler(cooldown=10.0)
        t.allow("pipeline_a", now=100.0)
        assert t.allow("pipeline_b", now=100.0) is True

    def test_max_firings_respected(self):
        t = self._throttler(cooldown=1.0, max_firings=2)
        assert t.allow("k", now=0.0) is True
        assert t.allow("k", now=2.0) is True
        assert t.allow("k", now=4.0) is False  # exceeded max

    def test_firing_count_increments(self):
        t = self._throttler(cooldown=1.0)
        assert t.firing_count("k") == 0
        t.allow("k", now=0.0)
        assert t.firing_count("k") == 1
        t.allow("k", now=2.0)
        assert t.firing_count("k") == 2

    def test_reset_clears_key(self):
        t = self._throttler(cooldown=100.0)
        t.allow("k", now=0.0)
        t.reset("k")
        assert t.allow("k", now=1.0) is True

    def test_reset_all_clears_everything(self):
        t = self._throttler(cooldown=100.0)
        t.allow("a", now=0.0)
        t.allow("b", now=0.0)
        t.reset_all()
        assert t.allow("a", now=1.0) is True
        assert t.allow("b", now=1.0) is True

    def test_zero_cooldown_always_allows(self):
        t = self._throttler(cooldown=0.0)
        t.allow("k", now=0.0)
        assert t.allow("k", now=0.0) is True
