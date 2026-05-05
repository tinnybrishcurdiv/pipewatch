"""Baseline comparison: compare current pipeline metrics against a stored baseline."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Dict, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class BaselineDiff:
    pipeline: str
    baseline_success_rate: Optional[float]
    current_success_rate: Optional[float]
    baseline_throughput: Optional[float]
    current_throughput: Optional[float]

    @property
    def success_rate_delta(self) -> Optional[float]:
        if self.baseline_success_rate is None or self.current_success_rate is None:
            return None
        return self.current_success_rate - self.baseline_success_rate

    @property
    def throughput_delta(self) -> Optional[float]:
        if self.baseline_throughput is None or self.current_throughput is None:
            return None
        return self.current_throughput - self.baseline_throughput

    def __str__(self) -> str:
        sr = (
            f"{self.success_rate_delta:+.1f}%"
            if self.success_rate_delta is not None
            else "N/A"
        )
        tp = (
            f"{self.throughput_delta:+.2f}/s"
            if self.throughput_delta is not None
            else "N/A"
        )
        return f"{self.pipeline}: success_rate {sr}, throughput {tp}"


def save_baseline(metrics_map: Dict[str, PipelineMetrics], path: str) -> None:
    """Persist current metrics as a baseline JSON file."""
    data = {
        name: {
            "success_rate": m.success_rate,
            "throughput": m.throughput,
        }
        for name, m in metrics_map.items()
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def load_baseline(path: str) -> Dict[str, Dict[str, Optional[float]]]:
    """Load a previously saved baseline from disk."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def compare_to_baseline(
    metrics_map: Dict[str, PipelineMetrics],
    baseline: Dict[str, Dict[str, Optional[float]]],
) -> Dict[str, BaselineDiff]:
    """Return a diff for every pipeline present in either current or baseline."""
    all_names = set(metrics_map) | set(baseline)
    diffs: Dict[str, BaselineDiff] = {}
    for name in sorted(all_names):
        current = metrics_map.get(name)
        base = baseline.get(name, {})
        diffs[name] = BaselineDiff(
            pipeline=name,
            baseline_success_rate=base.get("success_rate"),
            current_success_rate=current.success_rate if current else None,
            baseline_throughput=base.get("throughput"),
            current_throughput=current.throughput if current else None,
        )
    return diffs
