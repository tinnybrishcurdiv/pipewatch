"""Tests for pipewatch.summary_display."""

from __future__ import annotations

from pipewatch.aggregator import AggregatedSummary
from pipewatch.summary_display import render_summary


def _summary(**kwargs) -> AggregatedSummary:
    defaults = dict(
        total_pipelines=3,
        healthy=2,
        degraded=1,
        failing=0,
        total_processed=300,
        total_errors=15,
        avg_success_rate=0.95,
        slowest_pipeline="pipeline_c",
        fastest_pipeline="pipeline_a",
    )
    defaults.update(kwargs)
    return AggregatedSummary(**defaults)


class TestRenderSummary:
    def test_returns_string(self):
        assert isinstance(render_summary(_summary()), str)

    def test_contains_pipeline_count(self):
        out = render_summary(_summary(total_pipelines=7))
        assert "7" in out

    def test_contains_healthy_label(self):
        out = render_summary(_summary(healthy=2))
        assert "healthy" in out

    def test_contains_degraded_label(self):
        out = render_summary(_summary(degraded=1))
        assert "degraded" in out

    def test_contains_failing_label(self):
        out = render_summary(_summary(failing=0))
        assert "failing" in out

    def test_total_processed_formatted(self):
        out = render_summary(_summary(total_processed=1000))
        assert "1,000" in out

    def test_na_when_no_success_rate(self):
        out = render_summary(_summary(avg_success_rate=None, total_processed=0, total_errors=0))
        assert "N/A" in out

    def test_success_rate_percentage_shown(self):
        out = render_summary(_summary(avg_success_rate=0.95))
        assert "95.0%" in out

    def test_slowest_pipeline_shown(self):
        out = render_summary(_summary(slowest_pipeline="pipe_z"))
        assert "pipe_z" in out

    def test_fastest_pipeline_shown(self):
        out = render_summary(_summary(fastest_pipeline="pipe_a", slowest_pipeline="pipe_z"))
        assert "pipe_a" in out

    def test_separator_lines_present(self):
        out = render_summary(_summary())
        assert "---" in out
