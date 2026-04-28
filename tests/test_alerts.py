"""Tests for pipewatch.alerts module."""

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.alerts import AlertRule, AlertFiring, evaluate_rules, _get_metric_value


def make_metrics(success: int = 80, error: int = 20) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name="pipe-a",
        success_count=success,
        error_count=error,
        last_seen=1_000_000.0,
    )


class TestAlertRule:
    def test_valid_rule_creates_ok(self):
        rule = AlertRule("low-success", "pipe-a", "success_rate", 0.9, "lt")
        assert rule.name == "low-success"

    def test_invalid_comparator_raises(self):
        with pytest.raises(ValueError, match="comparator"):
            AlertRule("r", "p", "success_rate", 0.5, "eq")

    def test_invalid_metric_raises(self):
        with pytest.raises(ValueError, match="Unknown metric"):
            AlertRule("r", "p", "latency", 100, "gt")


class TestGetMetricValue:
    def test_success_rate(self):
        m = make_metrics(80, 20)
        assert _get_metric_value(m, "success_rate") == pytest.approx(0.8)

    def test_throughput(self):
        m = make_metrics(80, 20)
        assert _get_metric_value(m, "throughput") == 100.0

    def test_error_count(self):
        m = make_metrics(80, 20)
        assert _get_metric_value(m, "error_count") == 20.0

    def test_unknown_metric_returns_none(self):
        m = make_metrics()
        assert _get_metric_value(m, "latency") is None


class TestEvaluateRules:
    def _metrics_map(self):
        return {"pipe-a": make_metrics(80, 20)}

    def test_fires_when_success_rate_below_threshold(self):
        rules = [AlertRule("low-sr", "pipe-a", "success_rate", 0.9, "lt")]
        firings = evaluate_rules(rules, self._metrics_map())
        assert len(firings) == 1
        assert firings[0].pipeline == "pipe-a"

    def test_no_fire_when_above_threshold(self):
        rules = [AlertRule("low-sr", "pipe-a", "success_rate", 0.5, "lt")]
        firings = evaluate_rules(rules, self._metrics_map())
        assert firings == []

    def test_fires_on_high_error_count(self):
        rules = [AlertRule("high-err", "pipe-a", "error_count", 10.0, "gt")]
        firings = evaluate_rules(rules, self._metrics_map())
        assert len(firings) == 1

    def test_missing_pipeline_skipped(self):
        rules = [AlertRule("r", "pipe-z", "success_rate", 0.9, "lt")]
        firings = evaluate_rules(rules, self._metrics_map())
        assert firings == []

    def test_multiple_rules_multiple_firings(self):
        rules = [
            AlertRule("low-sr", "pipe-a", "success_rate", 0.9, "lt"),
            AlertRule("high-err", "pipe-a", "error_count", 5.0, "gt"),
        ]
        firings = evaluate_rules(rules, self._metrics_map())
        assert len(firings) == 2

    def test_alert_firing_str_contains_pipeline(self):
        rules = [AlertRule("low-sr", "pipe-a", "success_rate", 0.9, "lt", message="Success too low")]
        firings = evaluate_rules(rules, self._metrics_map())
        assert "pipe-a" in str(firings[0])
        assert "Success too low" in str(firings[0])
