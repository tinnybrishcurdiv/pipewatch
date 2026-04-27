"""Metrics collector that aggregates pipeline stats over a sliding window."""

from collections import deque
from datetime import datetime, timedelta
from typing import Deque, List, Optional

from pipewatch.metrics import PipelineMetrics


class MetricsCollector:
    """Collects and aggregates pipeline metrics over a rolling time window."""

    def __init__(self, pipeline_name: str, window_seconds: int = 60) -> None:
        self.pipeline_name = pipeline_name
        self.window_seconds = window_seconds
        self._samples: Deque[PipelineMetrics] = deque()

    def record(self, metrics: PipelineMetrics) -> None:
        """Add a new metrics sample and evict stale entries."""
        self._samples.append(metrics)
        self._evict_stale()

    def _evict_stale(self) -> None:
        """Remove samples older than the configured window."""
        cutoff = datetime.utcnow() - timedelta(seconds=self.window_seconds)
        while self._samples and self._samples[0].timestamp < cutoff:
            self._samples.popleft()

    def latest(self) -> Optional[PipelineMetrics]:
        """Return the most recent sample, or None if no samples exist."""
        return self._samples[-1] if self._samples else None

    def window_samples(self) -> List[PipelineMetrics]:
        """Return all samples within the current window."""
        self._evict_stale()
        return list(self._samples)

    def aggregate(self) -> Optional[PipelineMetrics]:
        """Return a single aggregated metrics snapshot for the window."""
        samples = self.window_samples()
        if not samples:
            return None

        total_processed = sum(s.records_processed for s in samples)
        total_failed = sum(s.records_failed for s in samples)
        avg_latency = sum(s.latency_ms for s in samples) / len(samples)
        avg_throughput = sum(s.throughput_per_sec for s in samples) / len(samples)
        any_unhealthy = any(not s.is_healthy for s in samples)
        last_error = next(
            (s.error_message for s in reversed(samples) if s.error_message), None
        )

        return PipelineMetrics(
            name=self.pipeline_name,
            timestamp=samples[-1].timestamp,
            records_processed=total_processed,
            records_failed=total_failed,
            throughput_per_sec=avg_throughput,
            latency_ms=avg_latency,
            is_healthy=not any_unhealthy,
            error_message=last_error,
        )
