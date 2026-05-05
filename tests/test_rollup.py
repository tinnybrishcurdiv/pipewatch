"""Tests for pipewatch.rollup."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.rollup import RollupBucket, _truncate_to_hour, rollup_records, rollup_file


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _rec(pipeline: str, ts: str, success_rate: float, throughput: float) -> dict:
    return {"pipeline": pipeline, "recorded_at": ts,
            "success_rate": success_rate, "throughput": throughput}


HOUR1 = "2024-06-01T10:00:00+00:00"
HOUR1_MID = "2024-06-01T10:30:00+00:00"
HOUR2 = "2024-06-01T11:00:00+00:00"


# ---------------------------------------------------------------------------
# _truncate_to_hour
# ---------------------------------------------------------------------------

class TestTruncateToHour:
    def test_already_on_hour(self):
        assert _truncate_to_hour("2024-06-01T10:00:00+00:00") == "2024-06-01T10:00:00+00:00"

    def test_mid_hour_truncated(self):
        result = _truncate_to_hour("2024-06-01T10:45:30+00:00")
        assert result.startswith("2024-06-01T10:00:00")

    def test_z_suffix_accepted(self):
        result = _truncate_to_hour("2024-06-01T10:45:00Z")
        assert "10:00:00" in result


# ---------------------------------------------------------------------------
# rollup_records
# ---------------------------------------------------------------------------

class TestRollupRecords:
    def test_empty_returns_empty(self):
        assert rollup_records([]) == []

    def test_single_record_produces_one_bucket(self):
        recs = [_rec("pipe-a", HOUR1, 1.0, 50.0)]
        buckets = rollup_records(recs)
        assert len(buckets) == 1
        b = buckets[0]
        assert b.pipeline == "pipe-a"
        assert b.record_count == 1
        assert b.avg_success_rate == pytest.approx(1.0)
        assert b.avg_throughput == pytest.approx(50.0)

    def test_two_records_same_bucket_averaged(self):
        recs = [
            _rec("pipe-a", HOUR1, 0.8, 40.0),
            _rec("pipe-a", HOUR1_MID, 0.6, 60.0),
        ]
        buckets = rollup_records(recs)
        assert len(buckets) == 1
        b = buckets[0]
        assert b.record_count == 2
        assert b.avg_success_rate == pytest.approx(0.7)
        assert b.avg_throughput == pytest.approx(50.0)
        assert b.min_success_rate == pytest.approx(0.6)
        assert b.max_success_rate == pytest.approx(0.8)

    def test_two_hours_produce_two_buckets(self):
        recs = [
            _rec("pipe-a", HOUR1, 1.0, 10.0),
            _rec("pipe-a", HOUR2, 0.5, 20.0),
        ]
        buckets = rollup_records(recs)
        assert len(buckets) == 2

    def test_two_pipelines_separated(self):
        recs = [
            _rec("pipe-a", HOUR1, 1.0, 10.0),
            _rec("pipe-b", HOUR1, 0.9, 5.0),
        ]
        buckets = rollup_records(recs)
        names = {b.pipeline for b in buckets}
        assert names == {"pipe-a", "pipe-b"}

    def test_none_success_rate_excluded_from_avg(self):
        recs = [
            {"pipeline": "p", "recorded_at": HOUR1, "success_rate": None, "throughput": 10.0},
            {"pipeline": "p", "recorded_at": HOUR1_MID, "success_rate": 0.8, "throughput": 20.0},
        ]
        buckets = rollup_records(recs)
        assert buckets[0].avg_success_rate == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# rollup_file
# ---------------------------------------------------------------------------

class TestRollupFile:
    def test_writes_buckets_to_dst(self, tmp_path):
        src = tmp_path / "history.ndjson"
        dst = tmp_path / "rollup" / "out.ndjson"
        src.write_text(
            json.dumps(_rec("pipe-a", HOUR1, 1.0, 30.0)) + "\n" +
            json.dumps(_rec("pipe-a", HOUR1_MID, 0.9, 20.0)) + "\n"
        )
        count = rollup_file(src, dst)
        assert count == 1
        assert dst.exists()
        lines = [l for l in dst.read_text().splitlines() if l.strip()]
        assert len(lines) == 1
        obj = json.loads(lines[0])
        assert obj["pipeline"] == "pipe-a"
        assert obj["record_count"] == 2

    def test_appends_on_second_call(self, tmp_path):
        src = tmp_path / "h.ndjson"
        dst = tmp_path / "out.ndjson"
        src.write_text(json.dumps(_rec("p", HOUR1, 1.0, 10.0)) + "\n")
        rollup_file(src, dst)
        rollup_file(src, dst)
        lines = [l for l in dst.read_text().splitlines() if l.strip()]
        assert len(lines) == 2
