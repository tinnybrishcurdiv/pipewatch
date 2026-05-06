"""Load error-budget SLO targets from a dict / JSON config."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict

_DEFAULT_SLO = 0.99


@dataclass
class BudgetConfig:
    default_slo: float = _DEFAULT_SLO
    per_pipeline: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 < self.default_slo < 1.0:
            raise ValueError("default_slo must be between 0 and 1 exclusive")
        for name, slo in self.per_pipeline.items():
            if not 0.0 < slo < 1.0:
                raise ValueError(
                    f"SLO for '{name}' must be between 0 and 1 exclusive"
                )

    def slo_for(self, pipeline: str) -> float:
        """Return the SLO target for *pipeline*, falling back to the default."""
        return self.per_pipeline.get(pipeline, self.default_slo)


def budget_config_from_dict(data: dict) -> BudgetConfig:
    """Build a :class:`BudgetConfig` from a plain dictionary."""
    default_slo = float(data.get("default_slo", _DEFAULT_SLO))
    per_pipeline: Dict[str, float] = {
        k: float(v) for k, v in data.get("per_pipeline", {}).items()
    }
    return BudgetConfig(default_slo=default_slo, per_pipeline=per_pipeline)


def budget_config_from_json(json_str: str) -> BudgetConfig:
    """Parse a JSON string and return a :class:`BudgetConfig`."""
    return budget_config_from_dict(json.loads(json_str))


def default_budget_config() -> BudgetConfig:
    """Return a :class:`BudgetConfig` with all defaults."""
    return BudgetConfig()
