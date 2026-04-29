"""Tests for pipewatch.rate_limiter."""
import time
import pytest
from pipewatch.rate_limiter import BucketConfig, RateLimiter


# ---------------------------------------------------------------------------
# BucketConfig validation
# ---------------------------------------------------------------------------

class TestBucketConfig:
    def test_valid_config_ok(self):
        cfg = BucketConfig(capacity=5.0, refill_rate=1.0)
        assert cfg.capacity == 5.0

    def test_zero_capacity_raises(self):
        with pytest.raises(ValueError, match="capacity"):
            BucketConfig(capacity=0, refill_rate=1.0)

    def test_negative_refill_raises(self):
        with pytest.raises(ValueError, match="refill_rate"):
            BucketConfig(capacity=5.0, refill_rate=-1.0)


# ---------------------------------------------------------------------------
# RateLimiter behaviour
# ---------------------------------------------------------------------------

@pytest.fixture()
def limiter():
    return RateLimiter(BucketConfig(capacity=3.0, refill_rate=100.0))


class TestRateLimiter:
    def test_first_events_allowed(self, limiter):
        assert limiter.allow("pipe-a") is True
        assert limiter.allow("pipe-a") is True
        assert limiter.allow("pipe-a") is True

    def test_exceeds_capacity_blocked(self, limiter):
        for _ in range(3):
            limiter.allow("pipe-b")
        assert limiter.allow("pipe-b") is False

    def test_different_pipelines_independent(self, limiter):
        for _ in range(3):
            limiter.allow("pipe-x")
        # pipe-y should still have full bucket
        assert limiter.allow("pipe-y") is True

    def test_tokens_refill_over_time(self):
        # Very high refill rate so we don't have to wait long
        lim = RateLimiter(BucketConfig(capacity=1.0, refill_rate=1000.0))
        lim.allow("pipe-c")          # consume the only token
        assert lim.allow("pipe-c") is False
        time.sleep(0.002)            # ~2 tokens refilled
        assert lim.allow("pipe-c") is True

    def test_available_tokens_full_at_start(self, limiter):
        tokens = limiter.available_tokens("new-pipe")
        assert tokens == pytest.approx(3.0, abs=0.01)

    def test_available_decreases_after_consume(self, limiter):
        limiter.allow("pipe-d")
        assert limiter.available_tokens("pipe-d") < 3.0

    def test_reset_restores_full_bucket(self, limiter):
        for _ in range(3):
            limiter.allow("pipe-e")
        assert limiter.allow("pipe-e") is False
        limiter.reset("pipe-e")
        assert limiter.allow("pipe-e") is True

    def test_reset_unknown_pipeline_noop(self, limiter):
        limiter.reset("ghost")  # should not raise
