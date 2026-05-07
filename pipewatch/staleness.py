"""Staleness detection: flag pipelines that have not emitted records recently."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class StalenessResult:
    pipeline: str
    last_seen: Optional[datetime]  # UTC
    age_seconds: Optional[float]
    threshold_seconds: float
    is_stale: bool

    def __str__(self) -> str:
        if self.last_seen is None:
            age_str = "never seen"
        else:
            age_str = f"{self.age_seconds:.1f}s ago"
        flag = "STALE" if self.is_stale else "ok"
        return f"{self.pipeline}: {flag} (last seen {age_str}, threshold {self.threshold_seconds:.0f}s)"


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def compute_staleness(
    metrics: PipelineMetrics,
    threshold_seconds: float = 300.0,
    now: Optional[datetime] = None,
) -> StalenessResult:
    """Return a StalenessResult for a single pipeline."""
    if threshold_seconds <= 0:
        raise ValueError("threshold_seconds must be positive")

    ref = now or _now_utc()

    last_seen: Optional[datetime] = None
    if metrics.records:
        ts = max(r.timestamp for r in metrics.records)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        last_seen = ts

    if last_seen is None:
        return StalenessResult(
            pipeline=metrics.pipeline,
            last_seen=None,
            age_seconds=None,
            threshold_seconds=threshold_seconds,
            is_stale=True,
        )

    age = (ref - last_seen).total_seconds()
    return StalenessResult(
        pipeline=metrics.pipeline,
        last_seen=last_seen,
        age_seconds=age,
        threshold_seconds=threshold_seconds,
        is_stale=age > threshold_seconds,
    )


def rank_by_staleness(
    results: List[StalenessResult],
) -> List[StalenessResult]:
    """Return results sorted: stale first, then by age descending (None age last)."""
    def _sort_key(r: StalenessResult):
        stale_first = 0 if r.is_stale else 1
        age = -(r.age_seconds if r.age_seconds is not None else float("-inf"))
        return (stale_first, age)

    return sorted(results, key=_sort_key)
