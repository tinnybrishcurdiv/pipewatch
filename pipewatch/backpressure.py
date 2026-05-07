"""Backpressure detection: identifies pipelines where throughput is falling
behind expected capacity, signalling upstream congestion or slow consumers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from pipewatch.metrics import PipelineMetrics


@dataclass
class BackpressureResult:
    pipeline: str
    current_tps: Optional[float]
    expected_tps: float
    ratio: Optional[float]          # current / expected  (None when no data)
    is_backpressured: bool
    severity: str                   # "none" | "mild" | "severe"

    def __str__(self) -> str:
        if self.ratio is None:
            return f"{self.pipeline}: no data"
        pct = self.ratio * 100.0
        return (
            f"{self.pipeline}: {pct:.1f}% of expected throughput "
            f"({self.current_tps:.2f} / {self.expected_tps:.2f} tps) "
            f"[{self.severity}]"
        )


def _severity(ratio: Optional[float], mild_threshold: float, severe_threshold: float) -> str:
    if ratio is None:
        return "none"
    if ratio < severe_threshold:
        return "severe"
    if ratio < mild_threshold:
        return "mild"
    return "none"


def compute_backpressure(
    metrics: PipelineMetrics,
    expected_tps: float,
    mild_threshold: float = 0.80,
    severe_threshold: float = 0.50,
) -> BackpressureResult:
    """Compute backpressure status for a single pipeline."""
    if expected_tps <= 0:
        raise ValueError("expected_tps must be positive")
    if not (0 < severe_threshold < mild_threshold <= 1.0):
        raise ValueError(
            "thresholds must satisfy 0 < severe_threshold < mild_threshold <= 1.0"
        )

    records = metrics.records
    if not records:
        return BackpressureResult(
            pipeline=metrics.pipeline,
            current_tps=None,
            expected_tps=expected_tps,
            ratio=None,
            is_backpressured=False,
            severity="none",
        )

    total_elapsed = sum(r.duration_seconds for r in records if r.duration_seconds > 0)
    total_processed = sum(r.records_processed for r in records)
    current_tps = total_processed / total_elapsed if total_elapsed > 0 else 0.0

    ratio = current_tps / expected_tps
    sev = _severity(ratio, mild_threshold, severe_threshold)
    return BackpressureResult(
        pipeline=metrics.pipeline,
        current_tps=current_tps,
        expected_tps=expected_tps,
        ratio=ratio,
        is_backpressured=sev != "none",
        severity=sev,
    )


def rank_by_pressure(
    results: Sequence[BackpressureResult],
) -> list[BackpressureResult]:
    """Return results sorted worst (lowest ratio) first; None-ratio entries last."""
    return sorted(
        results,
        key=lambda r: (r.ratio is None, r.ratio if r.ratio is not None else 1.0),
    )
