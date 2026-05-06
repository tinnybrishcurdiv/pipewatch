"""Configuration helpers for the replay feature."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ReplayConfig:
    """Validated configuration for a replay run."""

    window: int = 60
    snapshot_every: int = 10
    max_records: Optional[int] = None

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError(f"window must be positive, got {self.window}")
        if self.snapshot_every < 0:
            raise ValueError(f"snapshot_every must be >= 0, got {self.snapshot_every}")
        if self.max_records is not None and self.max_records <= 0:
            raise ValueError(f"max_records must be positive, got {self.max_records}")


def replay_config_from_dict(data: dict) -> ReplayConfig:
    """Build a :class:`ReplayConfig` from a plain dictionary.

    Unknown keys are silently ignored so callers can pass a broader config
    blob without pre-filtering.
    """
    return ReplayConfig(
        window=int(data.get("window", 60)),
        snapshot_every=int(data.get("snapshot_every", 10)),
        max_records=int(data["max_records"]) if data.get("max_records") is not None else None,
    )


def replay_config_from_json(raw: str) -> ReplayConfig:
    """Parse a JSON string and return a :class:`ReplayConfig`."""
    return replay_config_from_dict(json.loads(raw))


def default_replay_config() -> ReplayConfig:
    """Return a :class:`ReplayConfig` with all defaults."""
    return ReplayConfig()


def replay_config_from_file(path: Path) -> ReplayConfig:
    """Load a :class:`ReplayConfig` from a JSON file on disk."""
    with path.open() as fh:
        data = json.load(fh)
    replay_section = data.get("replay", data)
    return replay_config_from_dict(replay_section)
