"""Trend analysis for pipeline metrics over time."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.metrics import PipelineMetrics


@dataclass
class TrendPoint:
    """A single data point in a trend series."""
    success_rate: Optional[float]
    throughput: float  # records/sec


@dataclass
class TrendResult:
    """Summary of trend direction for a pipeline."""
    pipeline: str
    direction: str        # 'improving', 'degrading', 'stable', 'unknown'
    delta_rate: Optional[float]  # percentage points change, None if unknown
    points: int

    def __str__(self) -> str:
        arrow = {"improving": "↑", "degrading": "↓", "stable": "→", "unknown": "?"}.get(
            self.direction, "?"
        )
        delta = f"{self.delta_rate:+.1f}pp" if self.delta_rate is not None else "n/a"
        return f"{self.pipeline}: {arrow} {self.direction} ({delta}, n={self.points})"


def _to_point(m: PipelineMetrics) -> TrendPoint:
    total = m.success_count + m.failure_count
    rate = (m.success_count / total * 100.0) if total > 0 else None
    return TrendPoint(success_rate=rate, throughput=m.throughput or 0.0)


def compute_trend(
    history: Sequence[PipelineMetrics],
    min_points: int = 2,
    threshold: float = 2.0,
) -> TrendResult:
    """Compute trend direction from an ordered sequence of PipelineMetrics.

    Args:
        history: Oldest-first list of metrics snapshots for a single pipeline.
        min_points: Minimum snapshots required to determine direction.
        threshold: Minimum percentage-point change to be considered non-stable.

    Returns:
        TrendResult with direction and delta.
    """
    if not history:
        raise ValueError("history must not be empty")

    pipeline = history[0].pipeline_name
    points = [_to_point(m) for m in history]
    rated = [p for p in points if p.success_rate is not None]

    if len(rated) < min_points:
        return TrendResult(pipeline=pipeline, direction="unknown", delta_rate=None, points=len(rated))

    first_rate = rated[0].success_rate
    last_rate = rated[-1].success_rate
    delta = last_rate - first_rate  # type: ignore[operator]

    if abs(delta) < threshold:
        direction = "stable"
    elif delta > 0:
        direction = "improving"
    else:
        direction = "degrading"

    return TrendResult(pipeline=pipeline, direction=direction, delta_rate=round(delta, 2), points=len(rated))


def rank_by_trend(results: List[TrendResult]) -> List[TrendResult]:
    """Return results sorted: degrading first, then stable, then improving."""
    order = {"degrading": 0, "stable": 1, "improving": 2, "unknown": 3}
    return sorted(results, key=lambda r: order.get(r.direction, 3))
