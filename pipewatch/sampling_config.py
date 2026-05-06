"""Load SamplingPolicy from dict / JSON configuration."""
from __future__ import annotations

import json
from typing import Any, Dict

from pipewatch.sampling import SamplingPolicy

_DEFAULTS: Dict[str, Any] = {
    "base_rate": 1.0,
    "min_rate": 0.05,
    "high_volume_threshold": 500,
    "seed": None,
}


def _parse_policy(raw: Dict[str, Any]) -> SamplingPolicy:
    cfg = {**_DEFAULTS, **raw}
    return SamplingPolicy(
        base_rate=float(cfg["base_rate"]),
        min_rate=float(cfg["min_rate"]),
        high_volume_threshold=int(cfg["high_volume_threshold"]),
        seed=cfg["seed"],
    )


def sampling_policy_from_dict(data: Dict[str, Any]) -> SamplingPolicy:
    """Build a SamplingPolicy from a plain dict (e.g. parsed YAML/JSON config)."""
    sampling_section = data.get("sampling", {})
    return _parse_policy(sampling_section)


def sampling_policy_from_json(json_str: str) -> SamplingPolicy:
    """Build a SamplingPolicy from a raw JSON string."""
    return sampling_policy_from_dict(json.loads(json_str))


def default_sampling_policy() -> SamplingPolicy:
    """Return a SamplingPolicy with all defaults applied."""
    return _parse_policy({})
