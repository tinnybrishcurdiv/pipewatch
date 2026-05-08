"""Detect configuration or behavioural drift between two pipeline snapshots."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class DriftResult:
    pipeline: str
    success_rate_before: Optional[float]
    success_rate_after: Optional[float]
    throughput_before: Optional[float]
    throughput_after: Optional[float]
    drifted: bool
    reason: str

    def __str__(self) -> str:
        status = "DRIFTED" if self.drifted else "stable"
        return (
            f"{self.pipeline}: {status} — {self.reason} "
            f"(sr {self.success_rate_before:.1%} → {self.success_rate_after:.1%})"
            if self.success_rate_before is not None and self.success_rate_after is not None
            else f"{self.pipeline}: {status} — {self.reason}"
        )


def _rate(m: PipelineMetrics) -> Optional[float]:
    from pipewatch.metrics import success_rate
    return success_rate(m)


def _tps(m: PipelineMetrics) -> Optional[float]:
    records = m.records
    if not records:
        return None
    total = sum(getattr(r, "count", 1) for r in records)
    durations = [getattr(r, "duration_seconds", None) for r in records]
    durations = [d for d in durations if d is not None and d > 0]
    if not durations:
        return None
    return total / sum(durations)


def compute_drift(
    before: Dict[str, PipelineMetrics],
    after: Dict[str, PipelineMetrics],
    sr_threshold: float = 0.05,
    tps_threshold: float = 0.20,
) -> List[DriftResult]:
    """Compare two metric snapshots and return drift results for all pipelines."""
    pipelines = sorted(set(before) | set(after))
    results: List[DriftResult] = []

    for name in pipelines:
        m_before = before.get(name)
        m_after = after.get(name)

        sr_b = _rate(m_before) if m_before else None
        sr_a = _rate(m_after) if m_after else None
        tps_b = _tps(m_before) if m_before else None
        tps_a = _tps(m_after) if m_after else None

        reasons: List[str] = []
        if sr_b is not None and sr_a is not None:
            if abs(sr_a - sr_b) >= sr_threshold:
                reasons.append(
                    f"success_rate Δ={sr_a - sr_b:+.1%} (threshold {sr_threshold:.1%})"
                )
        if tps_b is not None and tps_a is not None and tps_b > 0:
            rel = (tps_a - tps_b) / tps_b
            if abs(rel) >= tps_threshold:
                reasons.append(
                    f"throughput Δ={rel:+.1%} (threshold {tps_threshold:.1%})"
                )
        if m_before is None:
            reasons.append("pipeline appeared in after snapshot")
        elif m_after is None:
            reasons.append("pipeline missing from after snapshot")

        drifted = bool(reasons)
        reason = "; ".join(reasons) if reasons else "no significant change"
        results.append(
            DriftResult(
                pipeline=name,
                success_rate_before=sr_b,
                success_rate_after=sr_a,
                throughput_before=tps_b,
                throughput_after=tps_a,
                drifted=drifted,
                reason=reason,
            )
        )

    return results
