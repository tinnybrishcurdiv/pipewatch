"""Replay historical pipeline records through the metrics collector for debugging."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional

from pipewatch.metrics import PipelineMetrics
from pipewatch.collector import MetricsCollector


@dataclass
class ReplayResult:
    pipeline: str
    records_fed: int
    snapshots: List[dict] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"ReplayResult(pipeline={self.pipeline!r}, "
            f"records_fed={self.records_fed}, "
            f"snapshots={len(self.snapshots)})"
        )


def _iter_records(path: Path) -> Iterator[dict]:
    """Yield parsed JSON objects from a newline-delimited JSON file."""
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def replay_file(
    path: Path,
    pipeline: str,
    window: int = 60,
    snapshot_every: int = 10,
    max_records: Optional[int] = None,
) -> ReplayResult:
    """Feed records from *path* into a fresh collector and capture periodic snapshots.

    Args:
        path: Path to a newline-delimited JSON history file.
        pipeline: Pipeline name to filter records on.
        window: Collector sliding-window size in seconds.
        snapshot_every: Capture a collector snapshot every N records.
        max_records: Stop after feeding this many records (``None`` = no limit).

    Returns:
        A :class:`ReplayResult` with per-snapshot metric dicts.
    """
    collector = MetricsCollector(window_seconds=window)
    fed = 0
    snapshots: List[dict] = []

    for raw in _iter_records(path):
        if raw.get("pipeline") != pipeline:
            continue
        if max_records is not None and fed >= max_records:
            break

        metric = PipelineMetrics(
            pipeline=raw["pipeline"],
            success=raw.get("success", True),
            latency_ms=raw.get("latency_ms"),
            records_processed=raw.get("records_processed", 0),
        )
        collector.record(metric)
        fed += 1

        if snapshot_every > 0 and fed % snapshot_every == 0:
            latest = collector.latest(pipeline)
            if latest:
                from pipewatch.metrics import to_dict
                snapshots.append(to_dict(latest))

    return ReplayResult(pipeline=pipeline, records_fed=fed, snapshots=snapshots)
