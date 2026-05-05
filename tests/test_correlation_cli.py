"""Tests for pipewatch.correlation_cli."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from pipewatch.correlation_cli import build_correlation_parser, run_correlation_command


def _write_ndjson(path: Path, records: list) -> str:
    p = path / "history.ndjson"
    p.write_text("\n".join(json.dumps(r) for r in records))
    return str(p)


def _records():
    pipelines = {
        "alpha": [8, 9, 7, 10, 6],
        "beta":  [8, 9, 7, 10, 6],
        "gamma": [2, 1, 3,  0, 4],
    }
    rows = []
    for name, successes in pipelines.items():
        for s in successes:
            rows.append({"pipeline": name, "total": 10, "success": s, "failure": 10 - s})
    return rows


class TestBuildCorrelationParser:
    def test_defaults(self):
        p = build_correlation_parser()
        args = p.parse_args(["history.ndjson"])
        assert args.min_points == 3
        assert args.min_strength == "weak"
        assert args.as_json is False

    def test_custom_min_points(self):
        p = build_correlation_parser()
        args = p.parse_args(["history.ndjson", "--min-points", "5"])
        assert args.min_points == 5

    def test_json_flag(self):
        p = build_correlation_parser()
        args = p.parse_args(["history.ndjson", "--json"])
        assert args.as_json is True


class TestRunCorrelationCommand:
    def test_text_output_contains_pipelines(self, tmp_path, capsys):
        fpath = _write_ndjson(tmp_path, _records())
        parser = build_correlation_parser()
        args = parser.parse_args([fpath, "--min-strength", "none"])
        run_correlation_command(args)
        out = capsys.readouterr().out
        assert "alpha" in out
        assert "beta" in out

    def test_json_output_is_valid(self, tmp_path, capsys):
        fpath = _write_ndjson(tmp_path, _records())
        parser = build_correlation_parser()
        args = parser.parse_args([fpath, "--json", "--min-strength", "none"])
        run_correlation_command(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert isinstance(data, list)
        assert all("coefficient" in item for item in data)

    def test_min_strength_filters_weak(self, tmp_path, capsys):
        # Build a history that produces only weak correlations by using noisy data
        rows = []
        for s in [5, 6, 5, 6, 5]:
            rows.append({"pipeline": "p1", "total": 10, "success": s, "failure": 10 - s})
        for s in [6, 5, 6, 5, 6]:
            rows.append({"pipeline": "p2", "total": 10, "success": s, "failure": 10 - s})
        fpath = _write_ndjson(tmp_path, rows)
        parser = build_correlation_parser()
        args = parser.parse_args([fpath, "--min-strength", "strong", "--json"])
        run_correlation_command(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        for item in data:
            assert item["strength"] == "strong"

    def test_no_pairs_prints_message(self, tmp_path, capsys):
        # Only one pipeline — no pairs possible
        rows = [{"pipeline": "solo", "total": 10, "success": s, "failure": 10 - s}
                for s in [8, 9, 7, 10, 6]]
        fpath = _write_ndjson(tmp_path, rows)
        parser = build_correlation_parser()
        args = parser.parse_args([fpath])
        run_correlation_command(args)
        out = capsys.readouterr().out
        assert "No correlated" in out
