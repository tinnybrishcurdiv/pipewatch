"""Tests for pipewatch.aggregator."""

from __future__ import annotations

import pytest

from pipewatch.aggregator import aggregate, AggregatedSummary
from pipewatch.metrics import PipelineMetrics


def _m(
    processed: int = 100,
    errors: int = 0,
    latency: float = 10.0,
) -> PipelineMetrics:
    return PipelineMetrics(
        total_processed=processed,
        total_errors=errors,
        avg_latency_ms=latency,
        last_seen=None,
    )


class TestAggregateEmpty:
    def test_empty_map_returns_zero_summary(self):
        s = aggregate({})
        assert s.total_pipelines == 0
        assert s.healthy == 0
        assert s.avg_success_rate is None
        assert s.overall_success_rate is None

    def test_empty_slowest_and_fastest_are_none(self):
        s = aggregate({})
        assert s.slowest_pipeline is None
        assert s.fastest_pipeline is None


class TestAggregateMetrics:
    def test_total_pipelines_count(self):
        m = {"a": _m(), "b": _m(), "c": _m()}
        s = aggregate(m)
        assert s.total_pipelines == 3

    def test_total_processed_sums(self):
        m = {"a": _m(processed=50), "b": _m(processed=75)}
        s = aggregate(m)
        assert s.total_processed == 125

    def test_total_errors_sums(self):
        m = {"a": _m(errors=5), "b": _m(errors=10)}
        s = aggregate(m)
        assert s.total_errors == 15

    def test_overall_success_rate_all_good(self):
        m = {"a": _m(processed=100, errors=0)}
        s = aggregate(m)
        assert s.overall_success_rate == pytest.approx(1.0)

    def test_overall_success_rate_mixed(self):
        m = {"a": _m(processed=100, errors=20)}
        s = aggregate(m)
        assert s.overall_success_rate == pytest.approx(0.8)

    def test_avg_success_rate_computed(self):
        m = {
            "a": _m(processed=100, errors=0),
            "b": _m(processed=100, errors=50),
        }
        s = aggregate(m)
        assert s.avg_success_rate == pytest.approx(0.75)

    def test_slowest_pipeline_has_highest_latency(self):
        m = {"fast": _m(latency=5.0), "slow": _m(latency=200.0)}
        s = aggregate(m)
        assert s.slowest_pipeline == "slow"

    def test_fastest_pipeline_has_lowest_latency(self):
        m = {"fast": _m(latency=5.0), "slow": _m(latency=200.0)}
        s = aggregate(m)
        assert s.fastest_pipeline == "fast"

    def test_healthy_count(self):
        m = {"a": _m(processed=100, errors=0)}
        s = aggregate(m)
        assert s.healthy == 1

    def test_failing_count(self):
        m = {"a": _m(processed=100, errors=100)}
        s = aggregate(m)
        assert s.failing == 1
