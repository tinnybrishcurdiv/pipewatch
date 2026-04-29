"""Token-bucket rate limiter for controlling pipeline event ingestion rates."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class BucketConfig:
    """Configuration for a single token-bucket."""
    capacity: float        # maximum tokens
    refill_rate: float     # tokens added per second

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be > 0")
        if self.refill_rate <= 0:
            raise ValueError("refill_rate must be > 0")


@dataclass
class _Bucket:
    config: BucketConfig
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)

    def __post_init__(self) -> None:
        self._tokens = self.config.capacity
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.config.capacity,
            self._tokens + elapsed * self.config.refill_rate,
        )
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> bool:
        """Attempt to consume *tokens*. Returns True if allowed."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens


class RateLimiter:
    """Per-pipeline token-bucket rate limiter."""

    def __init__(self, default_config: BucketConfig) -> None:
        self._default_config = default_config
        self._buckets: Dict[str, _Bucket] = {}

    def _get_bucket(self, pipeline: str) -> _Bucket:
        if pipeline not in self._buckets:
            self._buckets[pipeline] = _Bucket(self._default_config)
        return self._buckets[pipeline]

    def allow(self, pipeline: str, tokens: float = 1.0) -> bool:
        """Return True if the event for *pipeline* is within the rate limit."""
        return self._get_bucket(pipeline).consume(tokens)

    def available_tokens(self, pipeline: str) -> float:
        """Return current available tokens for *pipeline*."""
        return self._get_bucket(pipeline).available

    def reset(self, pipeline: str) -> None:
        """Reset the bucket for *pipeline* to full capacity."""
        self._buckets.pop(pipeline, None)
