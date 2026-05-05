"""Tests for pipewatch.forecast and pipewatch.forecast_cli."""
from __future__ import annotations

import json
import math
import time
from pathlib import Path

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.forecast import forecast, ForecastResult, _linear_fit, _r_squared
from pipewatch.forecast_cli import build_forecast_parser, run_forecast_command


def _m(name: str = "pipe", success: int = 9, failed: int = 1, throughput: float = 100.0, ts: float | None = None) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=name,
        total_records=success + failed,
        successful_records=success,
        failed_records=failed,
        throughput_per_second=throughput,
        window_seconds=60,
        last_updated=ts or time.time(),
    )


# ---------------------------------------------------------------------------
# Unit tests for forecast()
# ---------------------------------------------------------------------------

class TestForecast:
    def test_empty_history_returns_insufficient(self):
        result = forecast([], "success_rate")
        assert result.predicted_value is None
        assert result.confidence == "insufficient_data"

    def test_single_record_returns_insufficient(self):
        result = forecast([_m()], "success_rate")
        assert result.predicted_value is None

    def test_stable_history_predicts_near_current(self):
        now = time.time()
        history = [_m(ts=now - 60 * i) for i in range(10, 0, -1)]
        result = forecast(history, "success_rate", horizon=60)
        assert result.predicted_value is not None
        # stable 90 % success rate should predict close to 0.9
        assert abs(result.predicted_value - 0.9) < 0.05

    def test_rising_throughput_predicts_higher(self):
        now = time.time()
        history = [
            _m(throughput=float(10 * i), ts=now - 60 * (10 - i))
            for i in range(1, 11)
        ]
        result = forecast(history, "throughput", horizon=60)
        assert result.predicted_value is not None
        assert result.slope is not None and result.slope > 0

    def test_confidence_high_for_perfect_linear_data(self):
        now = time.time()
        history = [_m(throughput=float(i * 5), ts=now - 600 + i * 60) for i in range(10)]
        result = forecast(history, "throughput", horizon=60)
        assert result.confidence in ("high", "medium")

    def test_str_contains_pipeline_name(self):
        now = time.time()
        history = [_m(name="alpha", ts=now - 60 * i) for i in range(5, 0, -1)]
        result = forecast(history, "success_rate")
        assert "alpha" in str(result)

    def test_insufficient_data_str(self):
        result = ForecastResult("pipe", "success_rate", 300, None, "insufficient_data", None)
        assert "insufficient data" in str(result)


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

class TestForecastCLI:
    def _write_ndjson(self, tmp_path: Path, records) -> str:
        p = tmp_path / "history.ndjson"
        lines = [json.dumps(r.__dict__) for r in records]
        p.write_text("\n".join(lines) + "\n")
        return str(p)

    def test_defaults(self):
        parser = build_forecast_parser()
        args = parser.parse_args(["myfile.ndjson"])
        assert args.metric == "success_rate"
        assert args.horizon == 300
        assert args.pipeline == "*"

    def test_run_returns_zero_on_valid_input(self, tmp_path):
        now = time.time()
        records = [_m(name="pipe", ts=now - 60 * i) for i in range(5, 0, -1)]
        path = self._write_ndjson(tmp_path, records)
        parser = build_forecast_parser()
        args = parser.parse_args([path, "--pipeline", "pipe"])
        assert run_forecast_command(args) == 0

    def test_run_json_output_is_valid(self, tmp_path, capsys):
        now = time.time()
        records = [_m(name="pipe", ts=now - 60 * i) for i in range(5, 0, -1)]
        path = self._write_ndjson(tmp_path, records)
        parser = build_forecast_parser()
        args = parser.parse_args([path, "--json"])
        rc = run_forecast_command(args)
        assert rc == 0
        out = capsys.readouterr().out
        data = json.loads(out)
        assert isinstance(data, list)
        assert data[0]["pipeline"] == "pipe"

    def test_missing_file_returns_one(self, tmp_path):
        parser = build_forecast_parser()
        args = parser.parse_args([str(tmp_path / "no.ndjson")])
        assert run_forecast_command(args) == 1

    def test_unknown_pipeline_returns_one(self, tmp_path):
        now = time.time()
        records = [_m(name="pipe", ts=now - 60 * i) for i in range(3, 0, -1)]
        path = self._write_ndjson(tmp_path, records)
        parser = build_forecast_parser()
        args = parser.parse_args([path, "--pipeline", "ghost"])
        assert run_forecast_command(args) == 1
