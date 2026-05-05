"""Tests for pipewatch.baseline and pipewatch.baseline_cli."""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from pipewatch.baseline import (
    BaselineDiff,
    compare_to_baseline,
    load_baseline,
    save_baseline,
)
from pipewatch.baseline_cli import build_baseline_parser, run_baseline_command
from pipewatch.metrics import PipelineMetrics


def _m(success_rate: float, throughput: float) -> PipelineMetrics:
    m = MagicMock(spec=PipelineMetrics)
    m.success_rate = success_rate
    m.throughput = throughput
    return m


# ---------------------------------------------------------------------------
# BaselineDiff
# ---------------------------------------------------------------------------

class TestBaselineDiff:
    def test_delta_computed_correctly(self):
        d = BaselineDiff("p", 90.0, 95.0, 1.0, 1.5)
        assert d.success_rate_delta == pytest.approx(5.0)
        assert d.throughput_delta == pytest.approx(0.5)

    def test_delta_none_when_baseline_missing(self):
        d = BaselineDiff("p", None, 95.0, None, 1.5)
        assert d.success_rate_delta is None
        assert d.throughput_delta is None

    def test_str_contains_pipeline_name(self):
        d = BaselineDiff("my-pipe", 80.0, 90.0, 2.0, 3.0)
        assert "my-pipe" in str(d)

    def test_str_shows_na_for_missing(self):
        d = BaselineDiff("p", None, 90.0, None, 1.0)
        assert "N/A" in str(d)


# ---------------------------------------------------------------------------
# save / load baseline
# ---------------------------------------------------------------------------

class TestSaveLoadBaseline:
    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / "baseline.json")
        metrics = {"pipe-a": _m(100.0, 5.0), "pipe-b": _m(80.0, 2.5)}
        save_baseline(metrics, path)  # type: ignore[arg-type]
        data = load_baseline(path)
        assert data["pipe-a"]["success_rate"] == pytest.approx(100.0)
        assert data["pipe-b"]["throughput"] == pytest.approx(2.5)

    def test_file_is_valid_json(self, tmp_path):
        path = str(tmp_path / "b.json")
        save_baseline({"x": _m(50.0, 1.0)}, path)  # type: ignore[arg-type]
        with open(path) as fh:
            parsed = json.load(fh)
        assert "x" in parsed


# ---------------------------------------------------------------------------
# compare_to_baseline
# ---------------------------------------------------------------------------

class TestCompareToBaseline:
    def test_improvement_is_positive_delta(self):
        current = {"p": _m(95.0, 3.0)}
        baseline = {"p": {"success_rate": 85.0, "throughput": 2.0}}
        diffs = compare_to_baseline(current, baseline)  # type: ignore[arg-type]
        assert diffs["p"].success_rate_delta == pytest.approx(10.0)

    def test_missing_pipeline_in_current(self):
        diffs = compare_to_baseline({}, {"p": {"success_rate": 80.0, "throughput": 1.0}})
        assert diffs["p"].current_success_rate is None

    def test_new_pipeline_not_in_baseline(self):
        current = {"new": _m(100.0, 4.0)}
        diffs = compare_to_baseline(current, {})  # type: ignore[arg-type]
        assert diffs["new"].baseline_success_rate is None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _make_manager(metrics_map):
    manager = MagicMock()
    snapshots = {}
    for name, m in metrics_map.items():
        snap = MagicMock()
        snap.collector.latest.return_value = m
        snapshots[name] = snap
    manager.snapshots = snapshots
    return manager


class TestBaselineCLI:
    def test_capture_writes_file(self, tmp_path):
        out = str(tmp_path / "b.json")
        manager = _make_manager({"p": _m(99.0, 2.0)})
        args = build_baseline_parser().parse_args(["capture", "--output", out])
        rc = run_baseline_command(args, manager)
        assert rc == 0
        assert os.path.exists(out)

    def test_compare_missing_baseline_returns_2(self, tmp_path):
        manager = _make_manager({})
        missing = str(tmp_path / "no.json")
        args = build_baseline_parser().parse_args(["compare", "--baseline", missing])
        rc = run_baseline_command(args, manager)
        assert rc == 2

    def test_compare_warn_below_triggers_exit_1(self, tmp_path):
        path = str(tmp_path / "b.json")
        save_baseline({"p": _m(90.0, 1.0)}, path)  # type: ignore[arg-type]
        manager = _make_manager({"p": _m(70.0, 1.0)})
        args = build_baseline_parser().parse_args(
            ["compare", "--baseline", path, "--warn-below", "5"]
        )
        rc = run_baseline_command(args, manager)
        assert rc == 1

    def test_compare_json_format(self, tmp_path, capsys):
        path = str(tmp_path / "b.json")
        save_baseline({"p": _m(80.0, 2.0)}, path)  # type: ignore[arg-type]
        manager = _make_manager({"p": _m(85.0, 2.5)})
        args = build_baseline_parser().parse_args(
            ["compare", "--baseline", path, "--format", "json"]
        )
        rc = run_baseline_command(args, manager)
        assert rc == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload[0]["pipeline"] == "p"
