"""Per-pipeline throughput quota tracking and enforcement."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class QuotaConfig:
    """Configuration for throughput quotas."""
    default_max_tps: float = 1000.0
    per_pipeline: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.default_max_tps <= 0:
            raise ValueError("default_max_tps must be positive")
        for name, limit in self.per_pipeline.items():
            if limit <= 0:
                raise ValueError(f"quota for '{name}' must be positive, got {limit}")

    def max_tps_for(self, pipeline: str) -> float:
        return self.per_pipeline.get(pipeline, self.default_max_tps)


@dataclass
class QuotaResult:
    pipeline: str
    current_tps: Optional[float]
    max_tps: float
    exceeded: bool
    utilisation_pct: Optional[float]

    def __str__(self) -> str:
        tps_str = f"{self.current_tps:.2f}" if self.current_tps is not None else "N/A"
        util_str = f"{self.utilisation_pct:.1f}%" if self.utilisation_pct is not None else "N/A"
        status = "EXCEEDED" if self.exceeded else "OK"
        return (
            f"{self.pipeline}: {tps_str} tps / {self.max_tps:.2f} max "
            f"({util_str} utilisation) [{status}]"
        )


def compute_quota(metrics: PipelineMetrics, config: QuotaConfig) -> QuotaResult:
    """Compute quota utilisation for a single pipeline."""
    max_tps = config.max_tps_for(metrics.pipeline)
    records = metrics.records
    if not records:
        return QuotaResult(
            pipeline=metrics.pipeline,
            current_tps=None,
            max_tps=max_tps,
            exceeded=False,
            utilisation_pct=None,
        )
    total = sum(r.records_processed for r in records)
    window_seconds = max((r.timestamp for r in records), default=0) - min(
        (r.timestamp for r in records), default=0
    )
    current_tps = total / window_seconds if window_seconds > 0 else float(total)
    utilisation_pct = (current_tps / max_tps) * 100.0
    return QuotaResult(
        pipeline=metrics.pipeline,
        current_tps=current_tps,
        max_tps=max_tps,
        exceeded=current_tps > max_tps,
        utilisation_pct=utilisation_pct,
    )


def rank_by_utilisation(results: list[QuotaResult]) -> list[QuotaResult]:
    """Return results sorted by utilisation descending (None last)."""
    return sorted(
        results,
        key=lambda r: r.utilisation_pct if r.utilisation_pct is not None else -1.0,
        reverse=True,
    )
