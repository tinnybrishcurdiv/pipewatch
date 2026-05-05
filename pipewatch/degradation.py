"""Detect pipeline degradation by comparing recent performance to a historical window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class DegradationResult:
    pipeline: str
    current_success_rate: Optional[float]
    baseline_success_rate: Optional[float]
    current_throughput: float
    baseline_throughput: float
    degraded: bool
    reason: str

    def __str__(self) -> str:
        if not self.degraded:
            return f"{self.pipeline}: OK"
        return f"{self.pipeline}: DEGRADED — {self.reason}"


def _mean(values: List[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def detect_degradation(
    pipeline: str,
    recent: List[PipelineMetrics],
    history: List[PipelineMetrics],
    success_rate_drop: float = 0.10,
    throughput_drop: float = 0.20,
) -> DegradationResult:
    """Compare recent metrics against historical baseline and flag degradation.

    Args:
        pipeline: Pipeline name.
        recent: Recent metric snapshots (e.g. last N minutes).
        history: Historical metric snapshots used as baseline.
        success_rate_drop: Fractional drop in success rate that triggers degradation.
        throughput_drop: Fractional drop in throughput that triggers degradation.

    Returns:
        DegradationResult describing whether degradation was detected.
    """
    from pipewatch.metrics import success_rate as calc_sr

    recent_sr_vals = [calc_sr(m) for m in recent if calc_sr(m) is not None]
    hist_sr_vals = [calc_sr(m) for m in history if calc_sr(m) is not None]

    cur_sr = _mean(recent_sr_vals)
    base_sr = _mean(hist_sr_vals)

    cur_tp = _mean([m.records_per_second for m in recent]) or 0.0
    base_tp = _mean([m.records_per_second for m in history]) or 0.0

    reasons = []

    if cur_sr is not None and base_sr is not None:
        if base_sr > 0 and (base_sr - cur_sr) / base_sr >= success_rate_drop:
            reasons.append(
                f"success rate dropped {(base_sr - cur_sr) * 100:.1f}pp "
                f"({base_sr * 100:.1f}% → {cur_sr * 100:.1f}%)"
            )

    if base_tp > 0 and (base_tp - cur_tp) / base_tp >= throughput_drop:
        reasons.append(
            f"throughput dropped {(base_tp - cur_tp) / base_tp * 100:.1f}% "
            f"({base_tp:.1f} → {cur_tp:.1f} rec/s)"
        )

    degraded = bool(reasons)
    reason = "; ".join(reasons) if reasons else "none"

    return DegradationResult(
        pipeline=pipeline,
        current_success_rate=cur_sr,
        baseline_success_rate=base_sr,
        current_throughput=cur_tp,
        baseline_throughput=base_tp,
        degraded=degraded,
        reason=reason,
    )
