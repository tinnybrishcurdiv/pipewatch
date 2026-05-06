"""SLO compliance report: compares each pipeline's success rate against its target."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics, success_rate


@dataclass
class SLOResult:
    pipeline: str
    target: float          # 0‑1
    actual: Optional[float]  # 0‑1 or None when no data
    compliant: bool

    def gap(self) -> Optional[float]:
        """Percentage points below target (negative = above target)."""
        if self.actual is None:
            return None
        return round((self.target - self.actual) * 100, 2)

    def __str__(self) -> str:
        status = "OK" if self.compliant else "BREACH"
        actual_str = f"{self.actual * 100:.1f}%" if self.actual is not None else "N/A"
        return (
            f"[{status}] {self.pipeline}: target={self.target*100:.1f}% "
            f"actual={actual_str} gap={self.gap()}pp"
        )


def _default_target(defaults: Dict[str, float], pipeline: str) -> float:
    return defaults.get(pipeline, defaults.get("*", 0.99))


def compute_slo_report(
    metrics_map: Dict[str, PipelineMetrics],
    targets: Optional[Dict[str, float]] = None,
) -> List[SLOResult]:
    """Return an SLOResult for every pipeline in *metrics_map*.

    *targets* maps pipeline name (or ``"*"`` wildcard) to a 0‑1 target.
    Missing entries fall back to the wildcard, then to 0.99.
    """
    if targets is None:
        targets = {}

    results: List[SLOResult] = []
    for name, m in sorted(metrics_map.items()):
        target = _default_target(targets, name)
        actual = success_rate(m)
        compliant = actual is not None and actual >= target
        results.append(SLOResult(pipeline=name, target=target, actual=actual, compliant=compliant))
    return results


def rank_by_gap(results: List[SLOResult]) -> List[SLOResult]:
    """Return results sorted worst gap first (None gaps go last)."""
    def _key(r: SLOResult):
        g = r.gap()
        return (g is None, g if g is not None else 0)

    return sorted(results, key=_key, reverse=True)
