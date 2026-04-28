"""Tests for pipewatch.exporter and pipewatch.snapshot."""

import json
import time

import pytest

from pipewatch.exporter import export_csv, export_json
from pipewatch.metrics import PipelineMetrics
from pipewatch.snapshot import PipelineSnapshot, SnapshotManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric(processed=10, failed=2, rate=5.0):
    return PipelineMetrics(
        processed=processed,
        failed=failed,
        throughput_per_sec=rate,
        window_seconds=60,
    )


# ---------------------------------------------------------------------------
# export_json
# ---------------------------------------------------------------------------

class TestExportJson:
    def test_returns_valid_json(self):
        data = export_json([_metric()], pipeline_name="etl")
        parsed = json.loads(data)
        assert parsed["pipeline"] == "etl"

    def test_records_length(self):
        metrics = [_metric(), _metric(processed=20)]
        parsed = json.loads(export_json(metrics, pipeline_name="pipe"))
        assert len(parsed["records"]) == 2

    def test_exported_at_override(self):
        ts = "2024-01-01T00:00:00+00:00"
        parsed = json.loads(export_json([_metric()], "p", exported_at=ts))
        assert parsed["exported_at"] == ts

    def test_empty_snapshots(self):
        parsed = json.loads(export_json([], pipeline_name="empty"))
        assert parsed["records"] == []


# ---------------------------------------------------------------------------
# export_csv
# ---------------------------------------------------------------------------

class TestExportCsv:
    def test_empty_returns_empty_string(self):
        assert export_csv([], pipeline_name="p") == ""

    def test_has_header_row(self):
        csv_str = export_csv([_metric()], pipeline_name="etl")
        header = csv_str.splitlines()[0]
        assert "pipeline" in header
        assert "processed" in header

    def test_pipeline_column_value(self):
        csv_str = export_csv([_metric()], pipeline_name="my-pipe")
        assert "my-pipe" in csv_str

    def test_row_count(self):
        csv_str = export_csv([_metric(), _metric()], pipeline_name="p")
        lines = [l for l in csv_str.splitlines() if l.strip()]
        # 1 header + 2 data rows
        assert len(lines) == 3


# ---------------------------------------------------------------------------
# SnapshotManager
# ---------------------------------------------------------------------------

class TestSnapshotManager:
    def test_register_returns_collector(self):
        mgr = SnapshotManager(window_seconds=30)
        col = mgr.register("alpha")
        assert col is not None

    def test_register_idempotent(self):
        mgr = SnapshotManager()
        c1 = mgr.register("alpha")
        c2 = mgr.register("alpha")
        assert c1 is c2

    def test_snapshot_unknown_pipeline_has_no_metrics(self):
        mgr = SnapshotManager()
        snap = mgr.snapshot("unknown")
        assert snap.metrics is None
        assert snap.name == "unknown"

    def test_all_snapshots_sorted(self):
        mgr = SnapshotManager()
        mgr.register("beta")
        mgr.register("alpha")
        names = [s.name for s in mgr.all_snapshots()]
        assert names == ["alpha", "beta"]

    def test_pipeline_names_property(self):
        mgr = SnapshotManager()
        mgr.register("z")
        mgr.register("a")
        assert mgr.pipeline_names == ["a", "z"]
