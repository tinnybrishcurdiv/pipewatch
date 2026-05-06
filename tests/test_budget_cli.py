"""Tests for pipewatch.budget_cli."""
from __future__ import annotations

import io
from unittest.mock import MagicMock

import pytest

from pipewatch.budget_cli import build_budget_parser, run_budget_command
from pipewatch.metrics import PipelineMetrics


def _make_metrics(total: int, failed: int) -> PipelineMetrics:
    return PipelineMetrics(
        total_records=total,
        failed_records=failed,
        processed_per_second=5.0,
        avg_latency_ms=20.0,
        last_seen=0.0,
    )


def _snapshot(data: dict):
    snap = MagicMock()
    snap.all_metrics.return_value = data
    return snap


class TestBuildBudgetParser:
    def test_defaults(self):
        parser = build_budget_parser()
        args = parser.parse_args([])
        assert args.slo == 0.99
        assert args.worst == 0
        assert args.config_json is None

    def test_custom_slo(self):
        parser = build_budget_parser()
        args = parser.parse_args(["--slo", "0.95"])
        assert args.slo == 0.95

    def test_worst_flag(self):
        parser = build_budget_parser()
        args = parser.parse_args(["--worst", "3"])
        assert args.worst == 3


class TestRunBudgetCommand:
    def _args(self, slo=0.99, worst=0, config_json=None):
        ns = MagicMock()
        ns.slo = slo
        ns.worst = worst
        ns.config_json = config_json
        return ns

    def test_no_data_prints_message(self):
        out = io.StringIO()
        run_budget_command(self._args(), _snapshot({}), out=out)
        assert "No pipeline data" in out.getvalue()

    def test_output_contains_pipeline_name(self):
        out = io.StringIO()
        snap = _snapshot({"pipe-x": _make_metrics(1000, 5)})
        run_budget_command(self._args(), snap, out=out)
        assert "pipe-x" in out.getvalue()

    def test_worst_limits_output(self):
        out = io.StringIO()
        snap = _snapshot({
            "a": _make_metrics(1000, 0),
            "b": _make_metrics(1000, 50),
            "c": _make_metrics(1000, 100),
        })
        run_budget_command(self._args(worst=1), snap, out=out)
        lines = [l for l in out.getvalue().splitlines() if l.strip()]
        assert len(lines) == 1

    def test_per_pipeline_slo_via_json(self):
        import json
        out = io.StringIO()
        cfg = json.dumps({"default_slo": 0.99, "per_pipeline": {"strict": 0.999}})
        snap = _snapshot({"strict": _make_metrics(10000, 5)})
        # With 0.999 SLO, allowed = floor(0.001 * 10000) = 10; actual = 5 → OK
        run_budget_command(self._args(config_json=cfg), snap, out=out)
        assert "OK" in out.getvalue()

    def test_exhausted_label_appears(self):
        out = io.StringIO()
        snap = _snapshot({"bad": _make_metrics(100, 50)})
        run_budget_command(self._args(slo=0.99), snap, out=out)
        assert "EXHAUSTED" in out.getvalue()
