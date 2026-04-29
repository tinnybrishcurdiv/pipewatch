"""Tests for pipewatch.pipeline_health."""

from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_health import (
    HealthScore,
    _grade,
    compute_health,
    rank_pipelines,
)


def _m(
    pipeline: str = "pipe",
    success: int = 0,
    failure: int = 0,
    avg_latency: float | None = None,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        success_count=success,
        failure_count=failure,
        avg_latency_ms=avg_latency,
        last_seen=None,
    )


class TestGrade:
    def test_perfect_score_is_A(self):
        assert _grade(100.0) == "A"

    def test_90_is_A(self):
        assert _grade(90.0) == "A"

    def test_75_is_B(self):
        assert _grade(75.0) == "B"

    def test_55_is_C(self):
        assert _grade(55.0) == "C"

    def test_35_is_D(self):
        assert _grade(35.0) == "D"

    def test_34_is_F(self):
        assert _grade(34.9) == "F"

    def test_zero_is_F(self):
        assert _grade(0.0) == "F"


class TestComputeHealth:
    def test_all_success_healthy_pipeline(self):
        h = compute_health(_m(success=100, failure=0))
        assert h.score == 100.0
        assert h.grade == "A"
        assert h.status == "healthy"
        assert h.success_rate == pytest.approx(1.0)

    def test_no_records_gives_low_score(self):
        h = compute_health(_m(success=0, failure=0))
        assert h.score == 0.0
        assert h.grade == "F"
        assert h.success_rate is None
        assert h.total_records == 0

    def test_all_failures_scores_only_activity(self):
        # 0 rate pts + 0 status pts + 10 activity pts = 10
        h = compute_health(_m(success=0, failure=50))
        assert h.score == pytest.approx(10.0)
        assert h.grade == "F"
        assert h.status == "failing"

    def test_half_success_rate(self):
        # 0.5 * 70 = 35 rate pts; status='degraded' -> 0; activity -> 10 => 45
        h = compute_health(_m(success=50, failure=50))
        assert h.score == pytest.approx(45.0)
        assert h.grade == "D"

    def test_pipeline_name_preserved(self):
        h = compute_health(_m(pipeline="my-pipe", success=10))
        assert h.pipeline == "my-pipe"

    def test_str_contains_grade_and_pipeline(self):
        h = compute_health(_m(pipeline="demo", success=100))
        text = str(h)
        assert "[A]" in text
        assert "demo" in text

    def test_error_count_matches_failure_count(self):
        h = compute_health(_m(success=80, failure=20))
        assert h.error_count == 20


class TestRankPipelines:
    def test_sorted_worst_first(self):
        metrics_map = {
            "good": _m(pipeline="good", success=100, failure=0),
            "bad": _m(pipeline="bad", success=0, failure=100),
            "mid": _m(pipeline="mid", success=50, failure=50),
        }
        ranked = rank_pipelines(metrics_map)
        names = [h.pipeline for h in ranked]
        assert names[0] == "bad"
        assert names[-1] == "good"

    def test_empty_map_returns_empty_list(self):
        assert rank_pipelines({}) == []

    def test_returns_list_of_health_scores(self):
        metrics_map = {"p": _m(pipeline="p", success=10)}
        result = rank_pipelines(metrics_map)
        assert len(result) == 1
        assert isinstance(result[0], HealthScore)
