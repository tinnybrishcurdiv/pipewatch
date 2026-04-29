"""Tests for pipewatch.history module."""

import json
import os
import tempfile

import pytest

from pipewatch.history import HistoryWriter, HistoryReader


SAMPLE_METRICS = {
    "total": 100,
    "success": 95,
    "failure": 5,
    "success_rate": 0.95,
    "status": "healthy",
}

SAMPLE_SNAPSHOT = {
    "exported_at": "2024-01-15T10:00:00+00:00",
    "pipelines": {
        "etl-main": SAMPLE_METRICS,
        "etl-secondary": {"total": 50, "success": 40, "failure": 10, "success_rate": 0.8, "status": "degraded"},
    },
}


@pytest.fixture
def tmp_path_file(tmp_path):
    return str(tmp_path / "test_history.jsonl")


class TestHistoryWriter:
    def test_record_creates_file(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record("etl-main", SAMPLE_METRICS)
        assert os.path.exists(tmp_path_file)

    def test_record_writes_valid_json(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record("etl-main", SAMPLE_METRICS, ts="2024-01-15T10:00:00+00:00")
        with open(tmp_path_file) as fh:
            entry = json.loads(fh.readline())
        assert entry["pipeline"] == "etl-main"
        assert entry["success_rate"] == 0.95
        assert entry["timestamp"] == "2024-01-15T10:00:00+00:00"

    def test_record_appends_multiple(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record("pipe-a", SAMPLE_METRICS)
        w.record("pipe-b", SAMPLE_METRICS)
        with open(tmp_path_file) as fh:
            lines = [l for l in fh if l.strip()]
        assert len(lines) == 2

    def test_record_all_writes_each_pipeline(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record_all(SAMPLE_SNAPSHOT)
        r = HistoryReader(tmp_path_file)
        entries = r.read_all()
        assert len(entries) == 2
        pipelines = {e["pipeline"] for e in entries}
        assert pipelines == {"etl-main", "etl-secondary"}

    def test_record_all_uses_exported_at_timestamp(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record_all(SAMPLE_SNAPSHOT)
        r = HistoryReader(tmp_path_file)
        for entry in r.read_all():
            assert entry["timestamp"] == "2024-01-15T10:00:00+00:00"


class TestHistoryReader:
    def test_read_all_empty_when_no_file(self, tmp_path_file):
        r = HistoryReader(tmp_path_file)
        assert r.read_all() == []

    def test_read_pipeline_filters_correctly(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record_all(SAMPLE_SNAPSHOT)
        r = HistoryReader(tmp_path_file)
        results = r.read_pipeline("etl-main")
        assert len(results) == 1
        assert results[0]["pipeline"] == "etl-main"

    def test_read_pipeline_unknown_returns_empty(self, tmp_path_file):
        w = HistoryWriter(tmp_path_file)
        w.record_all(SAMPLE_SNAPSHOT)
        r = HistoryReader(tmp_path_file)
        assert r.read_pipeline("nonexistent") == []

    def test_export_csv_creates_file(self, tmp_path_file, tmp_path):
        w = HistoryWriter(tmp_path_file)
        w.record_all(SAMPLE_SNAPSHOT)
        r = HistoryReader(tmp_path_file)
        csv_path = str(tmp_path / "out.csv")
        count = r.export_csv(csv_path)
        assert count == 2
        assert os.path.exists(csv_path)

    def test_export_csv_empty_returns_zero(self, tmp_path_file, tmp_path):
        r = HistoryReader(tmp_path_file)
        csv_path = str(tmp_path / "out.csv")
        count = r.export_csv(csv_path)
        assert count == 0
        assert not os.path.exists(csv_path)
