"""Cross-pipeline correlation: find pipelines whose success rates move together."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from pipewatch.metrics import PipelineMetrics


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    coefficient: float  # Pearson r in [-1, 1]
    strength: str       # "strong", "moderate", "weak", "none"

    def __str__(self) -> str:
        sign = "+" if self.coefficient >= 0 else ""
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"r={sign}{self.coefficient:.3f} ({self.strength})"
        )


def _strength(r: float) -> str:
    abs_r = abs(r)
    if abs_r >= 0.8:
        return "strong"
    if abs_r >= 0.5:
        return "moderate"
    if abs_r >= 0.2:
        return "weak"
    return "none"


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    """Return Pearson correlation coefficient or None if undefined."""
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def compute_correlations(
    history: Dict[str, List[PipelineMetrics]],
    min_points: int = 3,
) -> List[CorrelationResult]:
    """Compute pairwise Pearson correlations of success rates across pipelines."""
    from pipewatch.metrics import success_rate

    names = sorted(history.keys())
    series: Dict[str, List[float]] = {}
    for name in names:
        records = history[name]
        if len(records) >= min_points:
            series[name] = [success_rate(r) for r in records]

    results: List[CorrelationResult] = []
    keys = sorted(series.keys())
    for i, a in enumerate(keys):
        for b in keys[i + 1 :]:
            xs = series[a]
            ys = series[b]
            length = min(len(xs), len(ys))
            r = _pearson(xs[:length], ys[:length])
            if r is None:
                continue
            results.append(
                CorrelationResult(
                    pipeline_a=a,
                    pipeline_b=b,
                    coefficient=round(r, 6),
                    strength=_strength(r),
                )
            )
    results.sort(key=lambda c: abs(c.coefficient), reverse=True)
    return results
