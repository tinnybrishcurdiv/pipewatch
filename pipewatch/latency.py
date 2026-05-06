"""Latency percentile analysis for pipeline records."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.metrics import PipelineMetrics


@dataclass
class LatencyResult:
    pipeline: str
    p50: Optional[float]  # seconds
    p95: Optional[float]
    p99: Optional[float]
    sample_count: int

    def __str__(self) -> str:
        def _fmt(v: Optional[float]) -> str:
            return f"{v:.3f}s" if v is not None else "n/a"

        return (
            f"{self.pipeline}: "
            f"p50={_fmt(self.p50)}  "
            f"p95={_fmt(self.p95)}  "
            f"p99={_fmt(self.p99)}  "
            f"(n={self.sample_count})"
        )


def _percentile(sorted_values: List[float], pct: float) -> Optional[float]:
    """Return the *pct*-th percentile (0-100) from a pre-sorted list."""
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * pct / 100.0
    lo = int(k)
    hi = lo + 1
    if hi >= len(sorted_values):
        return sorted_values[lo]
    frac = k - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


def compute_latency(
    pipeline: str,
    records: Sequence[PipelineMetrics],
) -> LatencyResult:
    """Compute latency percentiles from a sequence of PipelineMetrics.

    Each record's ``avg_latency`` field (seconds) is used as the sample.
    Records where ``avg_latency`` is *None* are ignored.
    """
    samples = sorted(
        r.avg_latency for r in records if r.avg_latency is not None
    )
    n = len(samples)
    return LatencyResult(
        pipeline=pipeline,
        p50=_percentile(samples, 50),
        p95=_percentile(samples, 95),
        p99=_percentile(samples, 99),
        sample_count=n,
    )


def rank_by_latency(
    results: Sequence[LatencyResult],
    percentile: str = "p95",
) -> List[LatencyResult]:
    """Return *results* sorted worst (highest) latency first.

    *percentile* must be one of ``'p50'``, ``'p95'``, or ``'p99'``.
    Results with *None* for the chosen percentile are placed last.
    """
    if percentile not in ("p50", "p95", "p99"):
        raise ValueError(f"percentile must be p50, p95, or p99; got {percentile!r}")

    def _key(r: LatencyResult) -> float:
        v: Optional[float] = getattr(r, percentile)
        return v if v is not None else -1.0

    return sorted(results, key=_key, reverse=True)
