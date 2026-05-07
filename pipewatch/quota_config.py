"""Load QuotaConfig from dict / JSON."""
from __future__ import annotations

import json
from typing import Any, Dict

from pipewatch.quota import QuotaConfig


def _parse_quota_config(data: Dict[str, Any]) -> QuotaConfig:
    default_max_tps = float(data.get("default_max_tps", 1000.0))
    per_pipeline: Dict[str, float] = {}
    for entry in data.get("per_pipeline", []):
        name = entry["pipeline"]
        limit = float(entry["max_tps"])
        per_pipeline[name] = limit
    return QuotaConfig(default_max_tps=default_max_tps, per_pipeline=per_pipeline)


def quota_config_from_dict(data: Dict[str, Any]) -> QuotaConfig:
    """Build a QuotaConfig from a plain dictionary."""
    return _parse_quota_config(data)


def quota_config_from_json(json_str: str) -> QuotaConfig:
    """Build a QuotaConfig from a JSON string."""
    return _parse_quota_config(json.loads(json_str))


def default_quota_config() -> QuotaConfig:
    """Return a QuotaConfig with sensible defaults."""
    return QuotaConfig()
