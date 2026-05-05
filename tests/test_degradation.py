"""Tests for pipewatch.degradation."""
from __future__ import annotations

import time
from typing import List

import pytest

from pipewatch.degradation import detect_degradation, DegradationResult
from pipewatch.metrics import PipelineMetrics


def _m(
    pipeline: str = "pipe",
    success: int = 100,
    failure: int = 0,
    records_per_second: float = 10.0,
    timestamp: float | None = None,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        success=success,
        failure=failure,
        records_per_second=records_per_second,
        timestamp=timestamp or time.time(),
    )


class TestDetectDegradation:
    def test_no_degradation_when_equal(self):
        recent = [_m(success=90, failure=10, records_per_second=10.0)]
        history = [_m(success=90, failure=10, records_per_second=10.0)]
        result = detect_degradation("pipe", recent, history)
        assert not result.degraded
        assert result.reason == "none"

    def test_success_rate_drop_triggers_degradation(self):
        # history: 100% success, recent: 50% success — well above 10% threshold
        recent = [_m(success=50, failure=50, records_per_second=10.0)]
        history = [_m(success=100, failure=0, records_per_second=10.0)]
        result = detect_degradation("pipe", recent, history, success_rate_drop=0.10)
        assert result.degraded
        assert "success rate" in result.reason

    def test_throughput_drop_triggers_degradation(self):
        recent = [_m(success=100, failure=0, records_per_second=2.0)]
        history = [_m(success=100, failure=0, records_per_second=10.0)]
        result = detect_degradation("pipe", recent, history, throughput_drop=0.20)
        assert result.degraded
        assert "throughput" in result.reason

    def test_small_drop_does_not_trigger(self):
        # 5% success rate drop with 10% threshold → no degradation
        recent = [_m(success=95, failure=5)]
        history = [_m(success=100, failure=0)]
        result = detect_degradation("pipe", recent, history, success_rate_drop=0.10)
        assert not result.degraded

    def test_empty_recent_uses_zero_throughput(self):
        history = [_m(records_per_second=10.0)]
        result = detect_degradation("pipe", [], history, throughput_drop=0.20)
        assert result.degraded
        assert result.current_throughput == 0.0

    def test_empty_history_no_degradation(self):
        recent = [_m()]
        result = detect_degradation("pipe", recent, [])
        assert not result.degraded

    def test_both_drops_combine_in_reason(self):
        recent = [_m(success=50, failure=50, records_per_second=1.0)]
        history = [_m(success=100, failure=0, records_per_second=10.0)]
        result = detect_degradation("pipe", recent, history)
        assert result.degraded
        assert "success rate" in result.reason
        assert "throughput" in result.reason

    def test_str_ok(self):
        recent = [_m()]
        history = [_m()]
        result = detect_degradation("mypipe", recent, history)
        assert "mypipe" in str(result)
        assert "OK" in str(result)

    def test_str_degraded(self):
        recent = [_m(success=10, failure=90, records_per_second=1.0)]
        history = [_m(success=100, failure=0, records_per_second=10.0)]
        result = detect_degradation("mypipe", recent, history)
        assert "DEGRADED" in str(result)

    def test_result_stores_pipeline_name(self):
        result = detect_degradation("alpha", [_m()], [_m()])
        assert result.pipeline == "alpha"

    def test_baseline_success_rate_computed(self):
        history = [_m(success=80, failure=20)]
        result = detect_degradation("pipe", [_m()], history)
        assert result.baseline_success_rate == pytest.approx(0.80)
