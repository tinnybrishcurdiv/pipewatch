"""Tests for pipewatch.backpressure."""
from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.backpressure import (
    BackpressureResult,
    compute_backpressure,
    rank_by_pressure,
)


def _m(pipeline: str, records: list[dict]) -> PipelineMetrics:
    """Build a minimal PipelineMetrics from a list of record dicts."""
    from pipewatch.metrics import PipelineMetrics
    from unittest.mock import MagicMock

    recs = []
    for r in records:
        rec = MagicMock()
        rec.duration_seconds = r.get("duration", 1.0)
        rec.records_processed = r.get("processed", 0)
        rec.success = r.get("success", True)
        recs.append(rec)

    m = MagicMock(spec=PipelineMetrics)
    m.pipeline = pipeline
    m.records = recs
    return m


class TestComputeBackpressure:
    def test_no_records_returns_no_data(self):
        m = _m("pipe-a", [])
        result = compute_backpressure(m, expected_tps=100.0)
        assert result.current_tps is None
        assert result.ratio is None
        assert result.is_backpressured is False
        assert result.severity == "none"

    def test_healthy_pipeline_not_backpressured(self):
        # 1000 records in 10 s = 100 tps, expected 100 tps → ratio 1.0
        m = _m("pipe-b", [{"duration": 10.0, "processed": 1000}])
        result = compute_backpressure(m, expected_tps=100.0)
        assert result.ratio == pytest.approx(1.0)
        assert result.is_backpressured is False
        assert result.severity == "none"

    def test_mild_backpressure(self):
        # 70 tps vs 100 expected → ratio 0.70 → mild (< 0.80)
        m = _m("pipe-c", [{"duration": 10.0, "processed": 700}])
        result = compute_backpressure(m, expected_tps=100.0)
        assert result.ratio == pytest.approx(0.70)
        assert result.is_backpressured is True
        assert result.severity == "mild"

    def test_severe_backpressure(self):
        # 40 tps vs 100 expected → ratio 0.40 → severe (< 0.50)
        m = _m("pipe-d", [{"duration": 10.0, "processed": 400}])
        result = compute_backpressure(m, expected_tps=100.0)
        assert result.ratio == pytest.approx(0.40)
        assert result.is_backpressured is True
        assert result.severity == "severe"

    def test_invalid_expected_tps_raises(self):
        m = _m("pipe-e", [])
        with pytest.raises(ValueError, match="expected_tps"):
            compute_backpressure(m, expected_tps=0)

    def test_invalid_thresholds_raises(self):
        m = _m("pipe-f", [])
        with pytest.raises(ValueError, match="thresholds"):
            compute_backpressure(m, expected_tps=50.0, mild_threshold=0.3, severe_threshold=0.8)

    def test_str_contains_pipeline_name(self):
        m = _m("my-pipe", [{"duration": 5.0, "processed": 250}])
        result = compute_backpressure(m, expected_tps=100.0)
        assert "my-pipe" in str(result)

    def test_str_no_data(self):
        m = _m("empty-pipe", [])
        result = compute_backpressure(m, expected_tps=100.0)
        assert "no data" in str(result)


class TestRankByPressure:
    def test_worst_ratio_first(self):
        results = [
            BackpressureResult("a", 90.0, 100.0, 0.90, False, "none"),
            BackpressureResult("b", 40.0, 100.0, 0.40, True, "severe"),
            BackpressureResult("c", 70.0, 100.0, 0.70, True, "mild"),
        ]
        ranked = rank_by_pressure(results)
        assert [r.pipeline for r in ranked] == ["b", "c", "a"]

    def test_none_ratio_sorted_last(self):
        results = [
            BackpressureResult("x", None, 100.0, None, False, "none"),
            BackpressureResult("y", 30.0, 100.0, 0.30, True, "severe"),
        ]
        ranked = rank_by_pressure(results)
        assert ranked[0].pipeline == "y"
        assert ranked[1].pipeline == "x"
