"""Capacity planning estimates for pipelines based on historical throughput."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class CapacityResult:
    pipeline: str
    current_tps: Optional[float]   # records per second (avg throughput)
    peak_tps: Optional[float]      # max observed tps in window
    headroom_pct: Optional[float]  # (peak_capacity - peak_tps) / peak_capacity * 100
    peak_capacity: Optional[float] # user-supplied or estimated ceiling
    at_risk: bool = False

    def __str__(self) -> str:
        tps = f"{self.current_tps:.1f}" if self.current_tps is not None else "n/a"
        head = f"{self.headroom_pct:.1f}%" if self.headroom_pct is not None else "n/a"
        risk = " [AT RISK]" if self.at_risk else ""
        return f"{self.pipeline}: tps={tps} headroom={head}{risk}"


def compute_capacity(
    pipeline: str,
    metrics: PipelineMetrics,
    peak_capacity: Optional[float] = None,
    at_risk_threshold: float = 80.0,
) -> CapacityResult:
    """Compute capacity metrics for a single pipeline."""
    records = metrics.records
    if not records:
        return CapacityResult(
            pipeline=pipeline,
            current_tps=None,
            peak_tps=None,
            headroom_pct=None,
            peak_capacity=peak_capacity,
        )

    throughputs = [r.throughput for r in records if r.throughput is not None]
    if not throughputs:
        return CapacityResult(
            pipeline=pipeline,
            current_tps=None,
            peak_tps=None,
            headroom_pct=None,
            peak_capacity=peak_capacity,
        )

    current_tps = sum(throughputs) / len(throughputs)
    peak_tps = max(throughputs)

    headroom_pct: Optional[float] = None
    at_risk = False
    if peak_capacity and peak_capacity > 0:
        headroom_pct = max(0.0, (peak_capacity - peak_tps) / peak_capacity * 100.0)
        used_pct = 100.0 - headroom_pct
        at_risk = used_pct >= at_risk_threshold

    return CapacityResult(
        pipeline=pipeline,
        current_tps=current_tps,
        peak_tps=peak_tps,
        headroom_pct=headroom_pct,
        peak_capacity=peak_capacity,
        at_risk=at_risk,
    )


def rank_by_headroom(results: list[CapacityResult]) -> list[CapacityResult]:
    """Return results sorted by headroom ascending (most at-risk first)."""
    known = [r for r in results if r.headroom_pct is not None]
    unknown = [r for r in results if r.headroom_pct is None]
    return sorted(known, key=lambda r: r.headroom_pct) + unknown  # type: ignore[arg-type]
