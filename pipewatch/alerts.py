"""Alert rules and evaluation for pipeline health thresholds."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipewatch.metrics import PipelineMetrics, success_rate


@dataclass
class AlertRule:
    """Defines a threshold-based alert rule for a pipeline metric."""
    name: str
    pipeline: str
    metric: str          # 'success_rate' | 'throughput' | 'error_count'
    threshold: float
    comparator: str      # 'lt' | 'gt'
    message: str = ""

    def __post_init__(self):
        if self.comparator not in ("lt", "gt"):
            raise ValueError(f"comparator must be 'lt' or 'gt', got '{self.comparator}'")
        if self.metric not in ("success_rate", "throughput", "error_count"):
            raise ValueError(f"Unknown metric '{self.metric}'")


@dataclass
class AlertFiring:
    """Represents an alert that has been triggered."""
    rule: AlertRule
    current_value: float
    pipeline: str

    def __str__(self) -> str:
        label = self.rule.message or self.rule.name
        return (
            f"[ALERT] {self.pipeline} — {label}: "
            f"{self.rule.metric}={self.current_value:.2f} "
            f"({'<' if self.rule.comparator == 'lt' else '>'} {self.rule.threshold})"
        )


def _get_metric_value(metrics: PipelineMetrics, metric: str) -> Optional[float]:
    if metric == "success_rate":
        return success_rate(metrics)
    if metric == "throughput":
        total = metrics.success_count + metrics.error_count
        return float(total)
    if metric == "error_count":
        return float(metrics.error_count)
    return None


def evaluate_rules(
    rules: List[AlertRule],
    metrics_map: dict,
) -> List[AlertFiring]:
    """Evaluate alert rules against a dict of {pipeline_name: PipelineMetrics}."""
    firings: List[AlertFiring] = []
    for rule in rules:
        metrics = metrics_map.get(rule.pipeline)
        if metrics is None:
            continue
        value = _get_metric_value(metrics, rule.metric)
        if value is None:
            continue
        triggered = (
            (rule.comparator == "lt" and value < rule.threshold) or
            (rule.comparator == "gt" and value > rule.threshold)
        )
        if triggered:
            firings.append(AlertFiring(rule=rule, current_value=value, pipeline=rule.pipeline))
    return firings
