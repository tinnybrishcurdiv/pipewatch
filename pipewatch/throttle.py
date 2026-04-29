"""Rate throttling utilities for alert suppression and notification cooldowns."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ThrottlePolicy:
    """Defines how frequently an alert key may fire."""

    cooldown_seconds: float = 60.0
    max_firings: Optional[int] = None  # None means unlimited within window

    def __post_init__(self) -> None:
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")
        if self.max_firings is not None and self.max_firings < 1:
            raise ValueError("max_firings must be >= 1 if set")


@dataclass
class _AlertState:
    last_fired: float = 0.0
    firing_count: int = 0


class AlertThrottler:
    """Tracks per-key alert state and decides whether a new firing is allowed."""

    def __init__(self, policy: Optional[ThrottlePolicy] = None) -> None:
        self._policy: ThrottlePolicy = policy or ThrottlePolicy()
        self._state: Dict[str, _AlertState] = {}

    def allow(self, key: str, *, now: Optional[float] = None) -> bool:
        """Return True if the alert *key* is allowed to fire right now."""
        ts = now if now is not None else time.monotonic()
        state = self._state.setdefault(key, _AlertState())

        elapsed = ts - state.last_fired
        if elapsed < self._policy.cooldown_seconds:
            return False

        if (
            self._policy.max_firings is not None
            and state.firing_count >= self._policy.max_firings
        ):
            return False

        # Allow — record the firing
        state.last_fired = ts
        state.firing_count += 1
        return True

    def reset(self, key: str) -> None:
        """Clear throttle state for *key* (e.g. when an alert resolves)."""
        self._state.pop(key, None)

    def reset_all(self) -> None:
        """Clear all throttle state."""
        self._state.clear()

    def firing_count(self, key: str) -> int:
        """Return how many times *key* has been allowed to fire."""
        return self._state.get(key, _AlertState()).firing_count
