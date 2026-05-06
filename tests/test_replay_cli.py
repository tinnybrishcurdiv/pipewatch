"""Tests for pipewatch.replay_cli."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.replay_cli import build_replay_parser, run_replay_command


def _write_ndjson(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _records(pipeline: str = "etl", n: int = 20) -> list:
    return [
        {"pipeline": pipeline, "success": True, "latency_ms": 40.0, "records_processed": 50}
        for _ in range(n)
    ]


class TestBuildReplayParser:
    def test_defaults(self):
        p = build_replay_parser()
        args = p.parse_args(["history.ndjson", "etl"])
        assert args.window == 60
        assert args.snapshot_every == 10
        assert args.max_records is None
        assert args.output_json is False

    def test_custom_window(self):
        p = build_replay_parser()
        args = p.parse_args(["f.ndjson", "pipe", "--window", "120"])
        assert args.window == 120

    def test_max_records(self):
        p = build_replay_parser()
        args = p.parse_args(["f.ndjson", "pipe", "--max-records", "5"])
        assert args.max_records == 5

    def test_json_flag(self):
        p = build_replay_parser()
        args = p.parse_args(["f.ndjson", "pipe", "--json"])
        assert args.output_json is True


class TestRunReplayCommand:
    def test_missing_file_returns_1(self, tmp_path):
        p = build_replay_parser()
        args = p.parse_args([str(tmp_path / "nope.ndjson"), "etl"])
        assert run_replay_command(args) == 1

    def test_valid_file_returns_0(self, tmp_path, capsys):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _records())
        p = build_replay_parser()
        args = p.parse_args([str(f), "etl"])
        assert run_replay_command(args) == 0

    def test_text_output_contains_pipeline(self, tmp_path, capsys):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _records())
        p = build_replay_parser()
        args = p.parse_args([str(f), "etl"])
        run_replay_command(args)
        out = capsys.readouterr().out
        assert "etl" in out

    def test_json_output_is_valid(self, tmp_path, capsys):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _records(n=10))
        p = build_replay_parser()
        args = p.parse_args([str(f), "etl", "--json"])
        run_replay_command(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["pipeline"] == "etl"
        assert "records_fed" in data
        assert "snapshots" in data

    def test_snapshot_count_in_text(self, tmp_path, capsys):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _records(n=20))
        p = build_replay_parser()
        args = p.parse_args([str(f), "etl", "--snapshot-every", "10"])
        run_replay_command(args)
        out = capsys.readouterr().out
        assert "Snapshots: 2" in out
