"""Anomaly detection for pipeline metrics using simple statistical thresholds."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.metrics import PipelineMetrics


@dataclass(frozen=True)
class AnomalyResult:
    pipeline: str
    metric: str
    value: float
    mean: float
    std: float
    z_score: float
    severity: str  # "warning" | "critical"

    def __str__(self) -> str:
        return (
            f"[{self.severity.upper()}] {self.pipeline}/{self.metric} "
            f"value={self.value:.2f} z={self.z_score:.2f}"
        )


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _std(values: Sequence[float], mean: float) -> float:
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _z_score(value: float, mean: float, std: float) -> float:
    if std == 0.0:
        return 0.0
    return (value - mean) / std


def detect_anomalies(
    current: PipelineMetrics,
    history: Sequence[PipelineMetrics],
    warning_z: float = 2.0,
    critical_z: float = 3.0,
) -> List[AnomalyResult]:
    """Compare *current* metrics against *history* and return anomalies."""
    if len(history) < 2:
        return []

    results: List[AnomalyResult] = []

    checks = {
        "success_rate": lambda m: m.success_rate,
        "error_rate": lambda m: m.error_rate,
        "throughput": lambda m: float(m.total),
    }

    for metric_name, extractor in checks.items():
        hist_values = [extractor(m) for m in history]
        current_value = extractor(current)
        mu = _mean(hist_values)
        sigma = _std(hist_values, mu)
        z = _z_score(current_value, mu, sigma)
        abs_z = abs(z)
        if abs_z >= critical_z:
            severity = "critical"
        elif abs_z >= warning_z:
            severity = "warning"
        else:
            continue
        results.append(
            AnomalyResult(
                pipeline=current.pipeline,
                metric=metric_name,
                value=current_value,
                mean=mu,
                std=sigma,
                z_score=z,
                severity=severity,
            )
        )

    return results
