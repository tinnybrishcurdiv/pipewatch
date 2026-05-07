"""Dead-letter queue tracking for pipelines.

Tracks records that failed processing and could not be retried,
providing counts, rates, and severity classification.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


_SEVERITY_THRESHOLDS = {
    "critical": 0.10,  # >10% dead-letter rate
    "warning": 0.02,   # >2% dead-letter rate
    "healthy": 0.0,
}


@dataclass
class DeadLetterResult:
    pipeline: str
    total_processed: int
    dead_letter_count: int
    rate: Optional[float]  # fraction 0-1, or None if no data
    severity: str          # "healthy", "warning", "critical", "no_data"

    def __str__(self) -> str:
        if self.rate is None:
            return f"{self.pipeline}: no data"
        pct = self.rate * 100
        return (
            f"{self.pipeline}: {self.dead_letter_count} dead-letter "
            f"({pct:.1f}%) [{self.severity}]"
        )


def _severity(rate: Optional[float]) -> str:
    if rate is None:
        return "no_data"
    if rate > _SEVERITY_THRESHOLDS["critical"]:
        return "critical"
    if rate > _SEVERITY_THRESHOLDS["warning"]:
        return "warning"
    return "healthy"


def compute_dead_letter(pipeline: str, records: list) -> DeadLetterResult:
    """Compute dead-letter stats from a list of PipelineMetrics records."""
    if not records:
        return DeadLetterResult(
            pipeline=pipeline,
            total_processed=0,
            dead_letter_count=0,
            rate=None,
            severity="no_data",
        )

    total = sum(getattr(r, "total", 0) for r in records)
    dead = sum(getattr(r, "dead_letter", 0) for r in records)
    rate = (dead / total) if total > 0 else None
    return DeadLetterResult(
        pipeline=pipeline,
        total_processed=total,
        dead_letter_count=dead,
        rate=rate,
        severity=_severity(rate),
    )


def rank_by_dead_letter(results: List[DeadLetterResult]) -> List[DeadLetterResult]:
    """Return results sorted by dead-letter rate descending (no_data last)."""
    def _sort_key(r: DeadLetterResult):
        return (r.rate is None, -(r.rate or 0.0))

    return sorted(results, key=_sort_key)
