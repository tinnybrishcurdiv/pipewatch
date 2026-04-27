"""Tests for pipewatch.display rendering functions."""

from datetime import datetime

import pytest

from pipewatch.display import (
    format_rate,
    render_dashboard,
    render_header,
    render_pipeline_row,
)
from pipewatch.metrics import PipelineMetrics


def make_metrics(
    name: str = "my-pipeline",
    success: int = 10,
    failure: int = 0,
    last_updated: datetime = None,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        success_count=success,
        failure_count=failure,
        last_updated=last_updated or datetime(2024, 6, 1, 12, 0, 0),
    )


class TestFormatRate:
    def test_none_returns_na(self):
        result = format_rate(None)
        assert "N/A" in result

    def test_high_rate_contains_percentage(self):
        result = format_rate(1.0)
        assert "100.0%" in result

    def test_medium_rate(self):
        result = format_rate(0.75)
        assert "75.0%" in result

    def test_low_rate(self):
        result = format_rate(0.5)
        assert "50.0%" in result


class TestRenderPipelineRow:
    def test_contains_pipeline_name(self):
        m = make_metrics(name="etl-loader")
        row = render_pipeline_row(m)
        assert "etl-loader" in row

    def test_contains_success_and_failure_counts(self):
        m = make_metrics(success=42, failure=3)
        row = render_pipeline_row(m)
        assert "42" in row
        assert "3" in row

    def test_contains_last_updated_time(self):
        m = make_metrics(last_updated=datetime(2024, 6, 1, 9, 30, 15))
        row = render_pipeline_row(m)
        assert "09:30:15" in row

    def test_healthy_status_present(self):
        m = make_metrics(success=100, failure=0)
        row = render_pipeline_row(m)
        assert "healthy" in row

    def test_critical_status_present(self):
        m = make_metrics(success=1, failure=20)
        row = render_pipeline_row(m)
        assert "critical" in row


class TestRenderHeader:
    def test_contains_title(self):
        header = render_header()
        assert "PipeWatch" in header

    def test_contains_column_names(self):
        header = render_header()
        assert "PIPELINE" in header
        assert "SUCCESS" in header
        assert "FAILURE" in header
        assert "RATE" in header
        assert "STATUS" in header


class TestRenderDashboard:
    def test_empty_pipeline_list(self):
        output = render_dashboard([])
        assert "No pipelines" in output

    def test_single_pipeline_rendered(self):
        m = make_metrics(name="stream-processor")
        output = render_dashboard([m])
        assert "stream-processor" in output

    def test_multiple_pipelines_sorted(self):
        metrics = [
            make_metrics(name="z-pipeline"),
            make_metrics(name="a-pipeline"),
        ]
        output = render_dashboard(metrics)
        assert output.index("a-pipeline") < output.index("z-pipeline")

    def test_tracking_count_shown(self):
        metrics = [make_metrics(name=f"pipe-{i}") for i in range(3)]
        output = render_dashboard(metrics)
        assert "3 pipeline(s)" in output
