"""Tests for pipewatch.anomaly_cli."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.anomaly_cli import build_anomaly_parser, run_anomaly_command
from pipewatch.metrics import PipelineMetrics


def _write_ndjson(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _metric_dict(pipeline="pipe", success=90, failed=10, total=100):
    return {
        "pipeline": pipeline,
        "success": success,
        "failed": failed,
        "total": total,
        "success_rate": success / total,
        "error_rate": failed / total,
    }


class TestBuildAnomalyParser:
    def test_defaults(self):
        p = build_anomaly_parser()
        args = p.parse_args(["history.ndjson"])
        assert args.history_file == "history.ndjson"
        assert args.warning_z == 2.0
        assert args.critical_z == 3.0
        assert args.pipeline is None
        assert args.json is False

    def test_custom_thresholds(self):
        p = build_anomaly_parser()
        args = p.parse_args(["f.ndjson", "--warning-z", "1.5", "--critical-z", "2.5"])
        assert args.warning_z == 1.5
        assert args.critical_z == 2.5

    def test_pipeline_filter(self):
        p = build_anomaly_parser()
        args = p.parse_args(["f.ndjson", "--pipeline", "my-pipe"])
        assert args.pipeline == "my-pipe"


class TestRunAnomalyCommand:
    def test_missing_file_returns_1(self, tmp_path):
        p = build_anomaly_parser()
        args = p.parse_args([str(tmp_path / "nope.ndjson")])
        assert run_anomaly_command(args) == 1

    def test_no_anomalies_returns_0(self, tmp_path, capsys):
        path = tmp_path / "hist.ndjson"
        records = [_metric_dict(success=90, failed=10) for _ in range(5)]
        _write_ndjson(path, records)
        p = build_anomaly_parser()
        args = p.parse_args([str(path)])
        rc = run_anomaly_command(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "No anomalies" in out

    def test_anomaly_detected_returns_0_and_prints(self, tmp_path, capsys):
        path = tmp_path / "hist.ndjson"
        stable = [_metric_dict(success=95, failed=5) for _ in range(8)]
        bad = [_metric_dict(success=5, failed=95)]
        _write_ndjson(path, stable + bad)
        p = build_anomaly_parser()
        args = p.parse_args([str(path), "--warning-z", "1.0", "--critical-z", "2.0"])
        rc = run_anomaly_command(args)
        assert rc == 0
        out = capsys.readouterr().out
        assert "pipe" in out

    def test_json_output_is_valid(self, tmp_path, capsys):
        path = tmp_path / "hist.ndjson"
        stable = [_metric_dict(success=95, failed=5) for _ in range(8)]
        bad = [_metric_dict(success=5, failed=95)]
        _write_ndjson(path, stable + bad)
        p = build_anomaly_parser()
        args = p.parse_args([str(path), "--json", "--warning-z", "0.5"])
        run_anomaly_command(args)
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_pipeline_filter_isolates(self, tmp_path, capsys):
        path = tmp_path / "hist.ndjson"
        records = (
            [_metric_dict(pipeline="a", success=95, failed=5) for _ in range(8)]
            + [_metric_dict(pipeline="a", success=5, failed=95)]
            + [_metric_dict(pipeline="b", success=90, failed=10) for _ in range(5)]
        )
        _write_ndjson(path, records)
        p = build_anomaly_parser()
        args = p.parse_args([str(path), "--pipeline", "b", "--warning-z", "0.5"])
        run_anomaly_command(args)
        out = capsys.readouterr().out
        assert "pipeline=a" not in out or "a" not in out.replace("No anomalies", "")
