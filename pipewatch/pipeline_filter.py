"""Filtering utilities for selecting pipelines by name pattern or status."""

from __future__ import annotations

import fnmatch
from typing import Iterable, List, Optional

from pipewatch.metrics import PipelineMetrics, status_label


VALID_STATUSES = {"healthy", "degraded", "critical", "unknown"}


def filter_by_pattern(names: Iterable[str], pattern: str) -> List[str]:
    """Return pipeline names that match a shell-style glob pattern."""
    return [n for n in names if fnmatch.fnmatch(n, pattern)]


def filter_by_status(
    metrics_map: dict[str, PipelineMetrics],
    status: str,
) -> List[str]:
    """Return pipeline names whose current status matches the given label.

    Args:
        metrics_map: Mapping of pipeline name -> PipelineMetrics.
        status: One of 'healthy', 'degraded', 'critical', 'unknown'.

    Raises:
        ValueError: If *status* is not a recognised status label.
    """
    status = status.lower()
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Unknown status {status!r}. Valid values: {sorted(VALID_STATUSES)}"
        )
    return [
        name
        for name, m in metrics_map.items()
        if status_label(m).lower() == status
    ]


def apply_filters(
    metrics_map: dict[str, PipelineMetrics],
    pattern: Optional[str] = None,
    status: Optional[str] = None,
) -> dict[str, PipelineMetrics]:
    """Apply optional pattern and/or status filters and return a filtered map.

    Either or both filters may be *None*, in which case they are skipped.
    """
    result = dict(metrics_map)

    if pattern is not None:
        matched = set(filter_by_pattern(result.keys(), pattern))
        result = {k: v for k, v in result.items() if k in matched}

    if status is not None:
        matched = set(filter_by_status(result, status))
        result = {k: v for k, v in result.items() if k in matched}

    return result
