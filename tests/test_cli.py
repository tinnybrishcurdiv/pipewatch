"""Tests for pipewatch.cli — argument parsing and snapshot rendering."""

import pytest
from unittest.mock import patch, MagicMock
from io import StringIO

from pipewatch.cli import build_parser, render_snapshot, run
from pipewatch.collector import MetricsCollector
from pipewatch.metrics import PipelineMetrics


def make_collector_with_data() -> MetricsCollector:
    collector = MetricsCollector(window_seconds=60)
    collector.record(
        "etl-main",
        PipelineMetrics(
            records_processed=500,
            records_failed=10,
            processing_rate=120.5,
            latency_p99=0.34,
            timestamp=1_700_000_000.0,
        ),
    )
    return collector


class TestBuildParser:
    def test_defaults(self):
        parser = build_parser()
        args = parser.parse_args([])
        assert args.interval == 2.0
        assert args.window == 60
        assert args.once is False

    def test_custom_interval(self):
        parser = build_parser()
        args = parser.parse_args(["--interval", "5"])
        assert args.interval == 5.0

    def test_custom_window(self):
        parser = build_parser()
        args = parser.parse_args(["--window", "120"])
        assert args.window == 120

    def test_once_flag(self):
        parser = build_parser()
        args = parser.parse_args(["--once"])
        assert args.once is True


class TestRenderSnapshot:
    def test_empty_collector_prints_no_data_message(self, capsys):
        collector = MetricsCollector(window_seconds=60)
        render_snapshot(collector)
        captured = capsys.readouterr()
        assert "no pipeline data" in captured.out

    def test_snapshot_contains_pipeline_name(self, capsys):
        collector = make_collector_with_data()
        render_snapshot(collector)
        captured = capsys.readouterr()
        assert "etl-main" in captured.out

    def test_snapshot_contains_header(self, capsys):
        collector = MetricsCollector(window_seconds=60)
        render_snapshot(collector)
        captured = capsys.readouterr()
        # render_header output should be present
        assert len(captured.out.strip()) > 0


class TestRunOnce:
    def test_once_mode_exits_after_single_render(self, capsys):
        collector = make_collector_with_data()
        parser = build_parser()
        args = parser.parse_args(["--once"])
        run(collector, args)  # should return without looping
        captured = capsys.readouterr()
        assert "etl-main" in captured.out

    def test_once_mode_empty_pipeline(self, capsys):
        collector = MetricsCollector(window_seconds=60)
        parser = build_parser()
        args = parser.parse_args(["--once"])
        run(collector, args)
        captured = capsys.readouterr()
        assert "no pipeline data" in captured.out
