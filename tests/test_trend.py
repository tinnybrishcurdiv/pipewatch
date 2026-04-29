"""Tests for pipewatch.trend and pipewatch.trend_cli."""
from __future__ import annotations

import json
import argparse
from io import StringIO
from pathlib import Path

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.trend import TrendPoint, TrendResult, compute_trend, rank_by_trend
from pipewatch.trend_cli import build_trend_parser, run_trend_command


def _m(name: str, success: int, failure: int, throughput: float = 1.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        success_count=success,
        failure_count=failure,
        avg_latency_ms=None,
        throughput=throughput,
        window_seconds=60,
    )


class TestComputeTrend:
    def test_empty_history_raises(self):
        with pytest.raises(ValueError):
            compute_trend([])

    def test_single_point_is_unknown(self):
        result = compute_trend([_m("p", 90, 10)])
        assert result.direction == "unknown"
        assert result.delta_rate is None

    def test_stable_when_delta_below_threshold(self):
        history = [_m("p", 90, 10), _m("p", 91, 9)]
        result = compute_trend(history, threshold=2.0)
        assert result.direction == "stable"

    def test_improving_when_rate_rises(self):
        history = [_m("p", 70, 30), _m("p", 90, 10)]
        result = compute_trend(history, threshold=2.0)
        assert result.direction == "improving"
        assert result.delta_rate == pytest.approx(20.0)

    def test_degrading_when_rate_falls(self):
        history = [_m("p", 95, 5), _m("p", 60, 40)]
        result = compute_trend(history, threshold=2.0)
        assert result.direction == "degrading"
        assert result.delta_rate is not None and result.delta_rate < 0

    def test_unknown_when_no_rated_points(self):
        history = [_m("p", 0, 0), _m("p", 0, 0)]
        result = compute_trend(history)
        assert result.direction == "unknown"

    def test_pipeline_name_preserved(self):
        result = compute_trend([_m("my-pipe", 80, 20), _m("my-pipe", 85, 15)])
        assert result.pipeline == "my-pipe"

    def test_str_contains_direction(self):
        r = TrendResult(pipeline="p", direction="degrading", delta_rate=-5.0, points=3)
        assert "degrading" in str(r)
        assert "↓" in str(r)


class TestRankByTrend:
    def test_degrading_comes_first(self):
        results = [
            TrendResult("a", "improving", 5.0, 3),
            TrendResult("b", "degrading", -8.0, 3),
            TrendResult("c", "stable", 0.5, 3),
        ]
        ranked = rank_by_trend(results)
        assert ranked[0].direction == "degrading"
        assert ranked[-1].direction == "improving"


class TestTrendCli:
    def _write_jsonl(self, path: Path, records: list[dict]) -> None:
        with path.open("w") as fh:
            for r in records:
                fh.write(json.dumps(r) + "\n")

    def test_missing_file_returns_1(self, tmp_path):
        parser = build_trend_parser()
        args = parser.parse_args([str(tmp_path / "missing.jsonl")])
        assert run_trend_command(args) == 1

    def test_empty_file_returns_0(self, tmp_path):
        f = tmp_path / "h.jsonl"
        f.write_text("")
        parser = build_trend_parser()
        args = parser.parse_args([str(f)])
        out = StringIO()
        assert run_trend_command(args, out=out) == 0

    def test_output_contains_pipeline_name(self, tmp_path):
        f = tmp_path / "h.jsonl"
        rows = [
            {"pipeline_name": "etl", "success_count": 80, "failure_count": 20, "window_seconds": 60},
            {"pipeline_name": "etl", "success_count": 90, "failure_count": 10, "window_seconds": 60},
        ]
        self._write_jsonl(f, rows)
        parser = build_trend_parser()
        args = parser.parse_args([str(f)])
        out = StringIO()
        code = run_trend_command(args, out=out)
        assert code == 0
        assert "etl" in out.getvalue()

    def test_unknown_pipeline_filter_returns_1(self, tmp_path):
        f = tmp_path / "h.jsonl"
        rows = [{"pipeline_name": "etl", "success_count": 9, "failure_count": 1, "window_seconds": 60}]
        self._write_jsonl(f, rows)
        parser = build_trend_parser()
        args = parser.parse_args([str(f), "--pipeline", "ghost"])
        assert run_trend_command(args) == 1
