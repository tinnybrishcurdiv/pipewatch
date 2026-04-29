"""Tests for pipewatch.rate_limiter_config."""
import json
import pytest
from pipewatch.rate_limiter_config import (
    rate_limiter_from_dict,
    rate_limiter_from_json,
    default_rate_limiter,
)
from pipewatch.rate_limiter import BucketConfig


class TestRateLimiterFromDict:
    def test_empty_dict_uses_defaults(self):
        lim = rate_limiter_from_dict({})
        assert lim.available_tokens("x") == pytest.approx(10.0, abs=0.1)

    def test_custom_default_capacity(self):
        lim = rate_limiter_from_dict({"default": {"capacity": 20, "refill_rate": 1}})
        assert lim.available_tokens("y") == pytest.approx(20.0, abs=0.1)

    def test_per_pipeline_override(self):
        cfg = {
            "default": {"capacity": 10, "refill_rate": 1},
            "pipelines": {
                "slow-etl": {"capacity": 3, "refill_rate": 0.5}
            },
        }
        lim = rate_limiter_from_dict(cfg)
        # slow-etl should have capacity 3
        assert lim.available_tokens("slow-etl") == pytest.approx(3.0, abs=0.1)
        # other pipelines use default capacity 10
        assert lim.available_tokens("fast-etl") == pytest.approx(10.0, abs=0.1)

    def test_per_pipeline_blocks_after_capacity(self):
        cfg = {
            "default": {"capacity": 10, "refill_rate": 1},
            "pipelines": {"tiny": {"capacity": 2, "refill_rate": 1}},
        }
        lim = rate_limiter_from_dict(cfg)
        assert lim.allow("tiny") is True
        assert lim.allow("tiny") is True
        assert lim.allow("tiny") is False


class TestRateLimiterFromJson:
    def test_parses_valid_json(self):
        payload = json.dumps({"default": {"capacity": 7, "refill_rate": 2}})
        lim = rate_limiter_from_json(payload)
        assert lim.available_tokens("p") == pytest.approx(7.0, abs=0.1)

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            rate_limiter_from_json("not-json")


class TestDefaultRateLimiter:
    def test_returns_rate_limiter(self):
        lim = default_rate_limiter()
        assert lim.allow("pipe") is True

    def test_default_capacity_ten(self):
        lim = default_rate_limiter()
        assert lim.available_tokens("pipe") == pytest.approx(10.0, abs=0.1)
