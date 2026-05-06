"""Tests for pipewatch.capacity_cli."""
from __future__ import annotations

import io
import json
import tempfile
from unittest.mock import MagicMock

from pipewatch.capacity_cli import build_capacity_parser, run_capacity_command


def _make_metrics(throughputs: list[float]):
    from unittest.mock import MagicMock
    records = []
    for t in throughputs:
        r = MagicMock()
        r.throughput = t
        records.append(r)
    m = MagicMock()
    m.records = records
    return m


def _snapshot(throughputs: list[float]):
    snap = MagicMock()
    collector = MagicMock()
    collector.latest.return_value = _make_metrics(throughputs)
    snap.collector = collector
    return snap


def _manager(pipelines: dict[str, list[float]]):
    manager = MagicMock()
    manager.snapshots.return_value = {
        name: _snapshot(tps) for name, tps in pipelines.items()
    }
    return manager


class TestBuildCapacityParser:
    def test_defaults(self):
        p = build_capacity_parser()
        args = p.parse_args([])
        assert args.sort == "headroom"
        assert args.config is None
        assert args.at_risk_threshold is None

    def test_custom_sort(self):
        p = build_capacity_parser()
        args = p.parse_args(["--sort", "name"])
        assert args.sort == "name"

    def test_custom_threshold(self):
        p = build_capacity_parser()
        args = p.parse_args(["--at-risk-threshold", "90"])
        assert args.at_risk_threshold == 90.0


class TestRunCapacityCommand:
    def test_no_pipelines_prints_message(self):
        manager = MagicMock()
        manager.snapshots.return_value = {}
        p = build_capacity_parser()
        args = p.parse_args([])
        out = io.StringIO()
        run_capacity_command(args, manager, out=out)
        assert "No pipelines" in out.getvalue()

    def test_output_contains_pipeline_name(self):
        manager = _manager({"my-pipe": [10.0, 20.0]})
        p = build_capacity_parser()
        args = p.parse_args([])
        out = io.StringIO()
        run_capacity_command(args, manager, out=out)
        assert "my-pipe" in out.getvalue()

    def test_sort_by_name(self):
        manager = _manager({"beta": [5.0], "alpha": [5.0]})
        p = build_capacity_parser()
        args = p.parse_args(["--sort", "name"])
        out = io.StringIO()
        run_capacity_command(args, manager, out=out)
        lines = [l for l in out.getvalue().splitlines() if l.strip() and "---" not in l and "Pipeline" not in l]
        names = [l.split()[0] for l in lines]
        assert names == sorted(names)

    def test_config_file_respected(self):
        cfg = {"default_peak_capacity": 100, "at_risk_threshold": 50}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(cfg, f)
            fname = f.name
        manager = _manager({"pipe": [60.0]})
        p = build_capacity_parser()
        args = p.parse_args(["--config", fname])
        out = io.StringIO()
        run_capacity_command(args, manager, out=out)
        assert "pipe" in out.getvalue()

    def test_at_risk_threshold_override(self):
        manager = _manager({"pipe": [95.0]})
        p = build_capacity_parser()
        # No peak capacity configured, so at_risk should not fire
        args = p.parse_args(["--at-risk-threshold", "80"])
        out = io.StringIO()
        run_capacity_command(args, manager, out=out)
        assert "pipe" in out.getvalue()
