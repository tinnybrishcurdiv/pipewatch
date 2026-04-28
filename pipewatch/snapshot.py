"""Capture and manage named pipeline metric snapshots."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetrics


@dataclass
class PipelineSnapshot:
    """Holds the latest resolved metrics for a single named pipeline."""

    name: str
    metrics: Optional[PipelineMetrics]
    window_seconds: int


class SnapshotManager:
    """Manages multiple named collectors and produces snapshots on demand."""

    def __init__(self, window_seconds: int = 60) -> None:
        self._window = window_seconds
        self._collectors: Dict[str, MetricsCollector] = {}

    def register(self, name: str) -> MetricsCollector:
        """Register a new named pipeline collector.

        If a collector with the same name already exists it is returned as-is.
        """
        if name not in self._collectors:
            self._collectors[name] = MetricsCollector(window_seconds=self._window)
        return self._collectors[name]

    def collector(self, name: str) -> Optional[MetricsCollector]:
        """Return the collector for *name*, or None if not registered."""
        return self._collectors.get(name)

    def snapshot(self, name: str) -> PipelineSnapshot:
        """Return a snapshot for the named pipeline."""
        col = self._collectors.get(name)
        metrics = col.latest() if col is not None else None
        return PipelineSnapshot(name=name, metrics=metrics, window_seconds=self._window)

    def all_snapshots(self) -> List[PipelineSnapshot]:
        """Return snapshots for every registered pipeline, sorted by name."""
        return [self.snapshot(name) for name in sorted(self._collectors)]

    @property
    def pipeline_names(self) -> List[str]:
        """Sorted list of registered pipeline names."""
        return sorted(self._collectors)
