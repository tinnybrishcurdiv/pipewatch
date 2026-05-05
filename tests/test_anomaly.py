"""Tests for pipewatch.anomaly."""
from __future__ import annotations

import pytest

from pipewatch.anomaly import AnomalyResult, detect_anomalies
from pipewatch.metrics import PipelineMetrics


def _m(
    pipeline: str = "pipe",
    success: int = 90,
    failed: int = 10,
    total: int = 100,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        success=success,
        failed=failed,
        total=total,
        success_rate=success / total if total else 0.0,
        error_rate=failed / total if total else 0.0,
    )


class TestDetectAnomalies:
    def test_empty_history_returns_nothing(self):
        result = detect_anomalies(_m(), [])
        assert result == []

    def test_single_history_returns_nothing(self):
        result = detect_anomalies(_m(), [_m()])
        assert result == []

    def test_stable_history_no_anomaly(self):
        history = [_m(success=90, failed=10) for _ in range(10)]
        current = _m(success=89, failed=11)
        result = detect_anomalies(current, history)
        assert result == []

    def test_extreme_drop_triggers_critical(self):
        history = [_m(success=95, failed=5, total=100) for _ in range(10)]
        current = _m(success=10, failed=90, total=100)
        results = detect_anomalies(current, history, warning_z=2.0, critical_z=3.0)
        severities = {r.severity for r in results}
        assert "critical" in severities

    def test_moderate_deviation_triggers_warning(self):
        # Build history with slight variation so std is non-zero
        history = [
            _m(success=90 + i % 3, failed=10 - i % 3, total=100)
            for i in range(8)
        ]
        # A noticeably worse current reading
        current = _m(success=60, failed=40, total=100)
        results = detect_anomalies(current, history, warning_z=1.5, critical_z=10.0)
        assert any(r.severity in ("warning", "critical") for r in results)

    def test_result_fields_populated(self):
        history = [_m(success=90, failed=10, total=100) for _ in range(5)]
        current = _m(success=10, failed=90, total=100)
        results = detect_anomalies(current, history)
        assert results
        r = results[0]
        assert r.pipeline == "pipe"
        assert isinstance(r.z_score, float)
        assert r.metric in ("success_rate", "error_rate", "throughput")

    def test_str_representation(self):
        r = AnomalyResult(
            pipeline="my-pipe",
            metric="error_rate",
            value=0.9,
            mean=0.1,
            std=0.02,
            z_score=40.0,
            severity="critical",
        )
        text = str(r)
        assert "CRITICAL" in text
        assert "my-pipe" in text
        assert "error_rate" in text

    def test_zero_std_no_crash(self):
        history = [_m(success=100, failed=0, total=100) for _ in range(5)]
        current = _m(success=100, failed=0, total=100)
        results = detect_anomalies(current, history)
        assert results == []

    def test_custom_z_thresholds_respected(self):
        history = [_m(success=90 + i, failed=10 - i, total=100) for i in range(6)]
        current = _m(success=50, failed=50, total=100)
        strict = detect_anomalies(current, history, warning_z=0.1, critical_z=0.5)
        lenient = detect_anomalies(current, history, warning_z=100.0, critical_z=200.0)
        assert len(strict) >= len(lenient)
