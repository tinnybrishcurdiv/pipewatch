"""Load correlation analysis configuration from a dict or JSON string."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class CorrelationConfig:
    """Settings that control how correlation analysis is performed."""
    min_points: int = 3
    min_strength: str = "weak"  # none | weak | moderate | strong
    include_pipelines: List[str] = field(default_factory=list)  # empty = all
    exclude_pipelines: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        valid_strengths = {"none", "weak", "moderate", "strong"}
        if self.min_strength not in valid_strengths:
            raise ValueError(
                f"min_strength must be one of {sorted(valid_strengths)}, "
                f"got {self.min_strength!r}"
            )
        if self.min_points < 2:
            raise ValueError(f"min_points must be >= 2, got {self.min_points}")


def correlation_config_from_dict(data: Dict[str, Any]) -> CorrelationConfig:
    """Build a CorrelationConfig from a plain dictionary (e.g. parsed YAML/JSON)."""
    section = data.get("correlation", data)  # allow top-level or nested key
    return CorrelationConfig(
        min_points=int(section.get("min_points", 3)),
        min_strength=str(section.get("min_strength", "weak")),
        include_pipelines=list(section.get("include_pipelines", [])),
        exclude_pipelines=list(section.get("exclude_pipelines", [])),
    )


def correlation_config_from_json(raw: str) -> CorrelationConfig:
    """Build a CorrelationConfig from a JSON string."""
    return correlation_config_from_dict(json.loads(raw))


def default_correlation_config() -> CorrelationConfig:
    """Return a CorrelationConfig with all defaults."""
    return CorrelationConfig()
