"""Tests for pipewatch.replay and pipewatch.replay_cli."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.replay import replay_file, ReplayResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_ndjson(path: Path, records: list) -> None:
    with path.open("w") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _make_records(pipeline: str, n: int, success: bool = True) -> list:
    return [
        {
            "pipeline": pipeline,
            "success": success,
            "latency_ms": 50.0,
            "records_processed": 100,
        }
        for _ in range(n)
    ]


# ---------------------------------------------------------------------------
# ReplayResult
# ---------------------------------------------------------------------------

class TestReplayResult:
    def test_str_contains_pipeline(self):
        r = ReplayResult(pipeline="etl", records_fed=5)
        assert "etl" in str(r)

    def test_str_contains_records_fed(self):
        r = ReplayResult(pipeline="etl", records_fed=42)
        assert "42" in str(r)


# ---------------------------------------------------------------------------
# replay_file
# ---------------------------------------------------------------------------

class TestReplayFile:
    def test_empty_file_returns_zero(self, tmp_path):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, [])
        result = replay_file(f, "etl")
        assert result.records_fed == 0
        assert result.snapshots == []

    def test_filters_by_pipeline(self, tmp_path):
        f = tmp_path / "h.ndjson"
        recs = _make_records("etl", 5) + _make_records("other", 10)
        _write_ndjson(f, recs)
        result = replay_file(f, "etl")
        assert result.records_fed == 5

    def test_snapshot_every_ten(self, tmp_path):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _make_records("etl", 30))
        result = replay_file(f, "etl", snapshot_every=10)
        assert len(result.snapshots) == 3

    def test_max_records_limits_feed(self, tmp_path):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _make_records("etl", 50))
        result = replay_file(f, "etl", max_records=15, snapshot_every=5)
        assert result.records_fed == 15

    def test_snapshots_have_success_rate(self, tmp_path):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _make_records("etl", 10))
        result = replay_file(f, "etl", snapshot_every=10)
        assert len(result.snapshots) == 1
        assert "success_rate" in result.snapshots[0]

    def test_no_snapshot_when_zero_interval(self, tmp_path):
        f = tmp_path / "h.ndjson"
        _write_ndjson(f, _make_records("etl", 20))
        result = replay_file(f, "etl", snapshot_every=0)
        assert result.snapshots == []
        assert result.records_fed == 20
