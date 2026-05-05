"""Tests for pipewatch.retention and pipewatch.retention_cli."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.retention import RetentionPolicy, prune_file, prune_directory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(days_ago: float) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.isoformat()


def _write_records(path: Path, records: list) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# RetentionPolicy
# ---------------------------------------------------------------------------

class TestRetentionPolicy:
    def test_defaults(self):
        p = RetentionPolicy()
        assert p.max_age_days == 30
        assert p.max_records_per_pipeline is None

    def test_zero_age_raises(self):
        with pytest.raises(ValueError, match="max_age_days"):
            RetentionPolicy(max_age_days=0)

    def test_negative_age_raises(self):
        with pytest.raises(ValueError, match="max_age_days"):
            RetentionPolicy(max_age_days=-5)

    def test_zero_max_records_raises(self):
        with pytest.raises(ValueError, match="max_records_per_pipeline"):
            RetentionPolicy(max_records_per_pipeline=0)

    def test_cutoff_is_in_the_past(self):
        p = RetentionPolicy(max_age_days=7)
        assert p.cutoff < datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# prune_file
# ---------------------------------------------------------------------------

class TestPruneFile:
    def test_nonexistent_file_returns_zero(self, tmp_path):
        assert prune_file(tmp_path / "missing.jsonl", RetentionPolicy()) == 0

    def test_removes_old_records(self, tmp_path):
        f = tmp_path / "pipe.jsonl"
        records = [
            {"timestamp": _ts(40), "pipeline": "p"},
            {"timestamp": _ts(1), "pipeline": "p"},
        ]
        _write_records(f, records)
        removed = prune_file(f, RetentionPolicy(max_age_days=30))
        assert removed == 1
        remaining = [json.loads(l) for l in f.read_text().splitlines() if l.strip()]
        assert len(remaining) == 1
        assert remaining[0]["timestamp"] == records[1]["timestamp"]

    def test_keeps_all_fresh_records(self, tmp_path):
        f = tmp_path / "pipe.jsonl"
        records = [{"timestamp": _ts(i), "pipeline": "p"} for i in range(5)]
        _write_records(f, records)
        removed = prune_file(f, RetentionPolicy(max_age_days=30))
        assert removed == 0

    def test_max_records_trims_oldest(self, tmp_path):
        f = tmp_path / "pipe.jsonl"
        records = [{"timestamp": _ts(i), "pipeline": "p"} for i in range(5, 0, -1)]
        _write_records(f, records)
        removed = prune_file(f, RetentionPolicy(max_age_days=30, max_records_per_pipeline=2))
        assert removed == 3
        remaining = [json.loads(l) for l in f.read_text().splitlines() if l.strip()]
        assert len(remaining) == 2


# ---------------------------------------------------------------------------
# prune_directory
# ---------------------------------------------------------------------------

class TestPruneDirectory:
    def test_returns_results_per_file(self, tmp_path):
        for name in ("a.jsonl", "b.jsonl"):
            _write_records(
                tmp_path / name,
                [{"timestamp": _ts(60), "pipeline": name}],
            )
        results = prune_directory(tmp_path, RetentionPolicy(max_age_days=30))
        assert set(results.keys()) == {"a.jsonl", "b.jsonl"}
        assert all(v == 1 for v in results.values())

    def test_ignores_non_jsonl_files(self, tmp_path):
        (tmp_path / "notes.txt").write_text("hello")
        results = prune_directory(tmp_path, RetentionPolicy())
        assert "notes.txt" not in results
