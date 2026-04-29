"""Aggregate metrics across multiple pipelines for summary reporting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics, success_rate


@dataclass
class AggregatedSummary:
    """Summary statistics across a set of pipelines."""

    total_pipelines: int
    healthy: int
    degraded: int
    failing: int
    total_processed: int
    total_errors: int
    avg_success_rate: Optional[float]  # None if no data
    slowest_pipeline: Optional[str]
    fastest_pipeline: Optional[str]

    @property
    def overall_success_rate(self) -> Optional[float]:
        if self.total_processed == 0:
            return None
        good = self.total_processed - self.total_errors
        return good / self.total_processed


def aggregate(metrics_map: Dict[str, PipelineMetrics]) -> AggregatedSummary:
    """Compute an AggregatedSummary from a mapping of pipeline name -> metrics."""
    if not metrics_map:
        return AggregatedSummary(
            total_pipelines=0,
            healthy=0,
            degraded=0,
            failing=0,
            total_processed=0,
            total_errors=0,
            avg_success_rate=None,
            slowest_pipeline=None,
            fastest_pipeline=None,
        )

    healthy = degraded = failing = 0
    total_processed = total_errors = 0
    rates: List[float] = []
    throughputs: Dict[str, float] = {}

    for name, m in metrics_map.items():
        label = m.status_label if hasattr(m, "status_label") else _status(m)
        if label == "healthy":
            healthy += 1
        elif label == "degraded":
            degraded += 1
        else:
            failing += 1

        total_processed += m.total_processed
        total_errors += m.total_errors

        r = success_rate(m)
        if r is not None:
            rates.append(r)

        if m.avg_latency_ms is not None:
            throughputs[name] = m.avg_latency_ms

    avg_rate = sum(rates) / len(rates) if rates else None
    slowest = max(throughputs, key=throughputs.__getitem__) if throughputs else None
    fastest = min(throughputs, key=throughputs.__getitem__) if throughputs else None

    return AggregatedSummary(
        total_pipelines=len(metrics_map),
        healthy=healthy,
        degraded=degraded,
        failing=failing,
        total_processed=total_processed,
        total_errors=total_errors,
        avg_success_rate=avg_rate,
        slowest_pipeline=slowest,
        fastest_pipeline=fastest,
    )


def _status(m: PipelineMetrics) -> str:
    from pipewatch.metrics import status_label
    return status_label(m)
