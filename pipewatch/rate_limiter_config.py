"""Load RateLimiter configuration from dicts / JSON."""
from __future__ import annotations

import json
from typing import Any, Dict

from pipewatch.rate_limiter import BucketConfig, RateLimiter

_DEFAULT_CAPACITY = 10.0
_DEFAULT_REFILL_RATE = 1.0  # tokens per second


def _parse_bucket(raw: Dict[str, Any]) -> BucketConfig:
    return BucketConfig(
        capacity=float(raw.get("capacity", _DEFAULT_CAPACITY)),
        refill_rate=float(raw.get("refill_rate", _DEFAULT_REFILL_RATE)),
    )


def rate_limiter_from_dict(cfg: Dict[str, Any]) -> RateLimiter:
    """Build a RateLimiter from a config dict.

    Expected shape::

        {
          "default": {"capacity": 10, "refill_rate": 2},
          "pipelines": {
            "slow-etl": {"capacity": 5, "refill_rate": 0.5}
          }
        }

    Per-pipeline overrides are pre-consumed so the bucket starts with
    the correct config when first accessed.
    """
    default_raw = cfg.get("default", {})
    default_cfg = _parse_bucket(default_raw)
    limiter = RateLimiter(default_cfg)

    for pipeline, raw in cfg.get("pipelines", {}).items():
        bucket_cfg = _parse_bucket(raw)
        # Register a custom bucket by temporarily overriding default
        original = limiter._default_config
        limiter._default_config = bucket_cfg
        limiter._get_bucket(pipeline)  # creates bucket with custom cfg
        limiter._default_config = original

    return limiter


def rate_limiter_from_json(json_str: str) -> RateLimiter:
    """Build a RateLimiter from a JSON string."""
    return rate_limiter_from_dict(json.loads(json_str))


def default_rate_limiter() -> RateLimiter:
    """Return a RateLimiter with sensible defaults."""
    return RateLimiter(BucketConfig(capacity=_DEFAULT_CAPACITY,
                                    refill_rate=_DEFAULT_REFILL_RATE))
