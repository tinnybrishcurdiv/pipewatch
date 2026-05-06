"""Load capacity planning configuration from dict / JSON."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CapacityConfig:
    default_peak_capacity: Optional[float] = None
    at_risk_threshold: float = 80.0
    per_pipeline: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.at_risk_threshold <= 0 or self.at_risk_threshold > 100:
            raise ValueError("at_risk_threshold must be in (0, 100]")

    def peak_capacity_for(self, pipeline: str) -> Optional[float]:
        return self.per_pipeline.get(pipeline, self.default_peak_capacity)


def capacity_config_from_dict(data: dict) -> CapacityConfig:
    per_pipeline: dict[str, float] = {}
    for k, v in data.get("per_pipeline", {}).items():
        per_pipeline[k] = float(v)

    default_cap = data.get("default_peak_capacity")
    return CapacityConfig(
        default_peak_capacity=float(default_cap) if default_cap is not None else None,
        at_risk_threshold=float(data.get("at_risk_threshold", 80.0)),
        per_pipeline=per_pipeline,
    )


def capacity_config_from_json(text: str) -> CapacityConfig:
    return capacity_config_from_dict(json.loads(text))


def default_capacity_config() -> CapacityConfig:
    return CapacityConfig()
