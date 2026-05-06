"""Tests for pipewatch.latency."""
from __future__ import annotations

import pytest

from pipewatch.latency import LatencyResult, _percentile, compute_latency, rank_by_latency
from pipewatch.metrics import PipelineMetrics


def _m(avg_latency: float | None = None, **kwargs) -> PipelineMetrics:
    defaults = dict(
        pipeline="pipe",
        total=10,
        success=10,
        failure=0,
        avg_latency=avg_latency,
    )
    defaults.update(kwargs)
    return PipelineMetrics(**defaults)


# ---------------------------------------------------------------------------
# _percentile
# ---------------------------------------------------------------------------

class TestPercentile:
    def test_empty_returns_none(self):
        assert _percentile([], 50) is None

    def test_single_element(self):
        assert _percentile([3.0], 50) == pytest.approx(3.0)

    def test_median_of_two(self):
        assert _percentile([1.0, 3.0], 50) == pytest.approx(2.0)

    def test_p100_returns_max(self):
        assert _percentile([1.0, 2.0, 5.0], 100) == pytest.approx(5.0)

    def test_p0_returns_min(self):
        assert _percentile([1.0, 2.0, 5.0], 0) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# compute_latency
# ---------------------------------------------------------------------------

class TestComputeLatency:
    def test_empty_records_all_none(self):
        result = compute_latency("pipe", [])
        assert result.p50 is None
        assert result.p95 is None
        assert result.p99 is None
        assert result.sample_count == 0

    def test_none_latency_records_ignored(self):
        records = [_m(avg_latency=None), _m(avg_latency=None)]
        result = compute_latency("pipe", records)
        assert result.sample_count == 0
        assert result.p50 is None

    def test_single_record_all_percentiles_equal(self):
        result = compute_latency("pipe", [_m(avg_latency=1.5)])
        assert result.p50 == pytest.approx(1.5)
        assert result.p95 == pytest.approx(1.5)
        assert result.p99 == pytest.approx(1.5)
        assert result.sample_count == 1

    def test_multiple_records_p95_above_median(self):
        records = [_m(avg_latency=float(i)) for i in range(1, 101)]
        result = compute_latency("pipe", records)
        assert result.p50 == pytest.approx(50.5)
        assert result.p95 is not None
        assert result.p95 > result.p50  # type: ignore[operator]

    def test_pipeline_name_preserved(self):
        result = compute_latency("my_pipeline", [_m(avg_latency=0.1)])
        assert result.pipeline == "my_pipeline"

    def test_str_contains_pipeline(self):
        result = compute_latency("alpha", [_m(avg_latency=0.25)])
        assert "alpha" in str(result)

    def test_str_na_when_no_samples(self):
        result = compute_latency("beta", [])
        assert "n/a" in str(result)


# ---------------------------------------------------------------------------
# rank_by_latency
# ---------------------------------------------------------------------------

class TestRankByLatency:
    def _make_result(self, name, p95):
        return LatencyResult(pipeline=name, p50=p95, p95=p95, p99=p95, sample_count=1)

    def test_sorted_worst_first(self):
        results = [
            self._make_result("fast", 0.1),
            self._make_result("slow", 2.0),
            self._make_result("medium", 0.5),
        ]
        ranked = rank_by_latency(results, percentile="p95")
        assert [r.pipeline for r in ranked] == ["slow", "medium", "fast"]

    def test_none_latency_sorted_last(self):
        results = [
            LatencyResult("no_data", None, None, None, 0),
            self._make_result("fast", 0.1),
        ]
        ranked = rank_by_latency(results, percentile="p95")
        assert ranked[-1].pipeline == "no_data"

    def test_invalid_percentile_raises(self):
        with pytest.raises(ValueError, match="p50"):
            rank_by_latency([], percentile="p80")
