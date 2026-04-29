"""Pipeline health scoring: compute a composite health score for each pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics, success_rate, status_label


@dataclass
class HealthScore:
    """Composite health score for a single pipeline."""

    pipeline: str
    score: float          # 0.0 (worst) – 100.0 (best)
    grade: str            # A / B / C / D / F
    status: str           # healthy / degraded / failing / unknown
    success_rate: Optional[float]
    error_count: int
    total_records: int

    def __str__(self) -> str:
        sr = f"{self.success_rate:.1%}" if self.success_rate is not None else "N/A"
        return (
            f"[{self.grade}] {self.pipeline}: score={self.score:.1f}, "
            f"status={self.status}, success_rate={sr}, errors={self.error_count}"
        )


def _grade(score: float) -> str:
    """Map a numeric score to a letter grade."""
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 55:
        return "C"
    if score >= 35:
        return "D"
    return "F"


def compute_health(metrics: PipelineMetrics) -> HealthScore:
    """Compute a HealthScore from a PipelineMetrics snapshot.

    Scoring breakdown (100 pts total):
      - 70 pts  from success_rate  (linear)
      - 20 pts  if status is 'healthy'
      - 10 pts  if total_records > 0 (pipeline is active)
    """
    sr = success_rate(metrics)
    total = metrics.success_count + metrics.failure_count
    status = status_label(metrics)

    rate_score = (sr * 70.0) if sr is not None else 0.0
    status_score = 20.0 if status == "healthy" else 0.0
    activity_score = 10.0 if total > 0 else 0.0

    score = round(rate_score + status_score + activity_score, 2)
    score = max(0.0, min(100.0, score))

    return HealthScore(
        pipeline=metrics.pipeline,
        score=score,
        grade=_grade(score),
        status=status,
        success_rate=sr,
        error_count=metrics.failure_count,
        total_records=total,
    )


def rank_pipelines(metrics_map: dict[str, PipelineMetrics]) -> list[HealthScore]:
    """Return HealthScore list sorted by score ascending (worst first)."""
    scores = [compute_health(m) for m in metrics_map.values()]
    return sorted(scores, key=lambda h: h.score)
