"""Tests for pipewatch.slo_report and pipewatch.slo_report_cli."""
from __future__ import annotations

import json
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.slo_report import SLOResult, compute_slo_report, rank_by_gap


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _m(total: int, failed: int, rate_per_sec: float = 10.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline="p",
        total_records=total,
        failed_records=failed,
        rate_per_sec=rate_per_sec,
        window_seconds=60,
    )


# ---------------------------------------------------------------------------
# SLOResult
# ---------------------------------------------------------------------------

class TestSLOResult:
    def test_gap_above_target_is_negative(self):
        r = SLOResult(pipeline="p", target=0.90, actual=0.95, compliant=True)
        assert r.gap() == -5.0

    def test_gap_below_target_is_positive(self):
        r = SLOResult(pipeline="p", target=0.99, actual=0.95, compliant=False)
        assert r.gap() == pytest.approx(4.0, abs=0.01)

    def test_gap_none_when_no_actual(self):
        r = SLOResult(pipeline="p", target=0.99, actual=None, compliant=False)
        assert r.gap() is None

    def test_str_contains_status_ok(self):
        r = SLOResult(pipeline="orders", target=0.99, actual=1.0, compliant=True)
        assert "OK" in str(r)
        assert "orders" in str(r)

    def test_str_contains_breach(self):
        r = SLOResult(pipeline="orders", target=0.99, actual=0.90, compliant=False)
        assert "BREACH" in str(r)


# ---------------------------------------------------------------------------
# compute_slo_report
# ---------------------------------------------------------------------------

class TestComputeSloReport:
    def test_empty_map_returns_empty_list(self):
        assert compute_slo_report({}) == []

    def test_compliant_when_above_target(self):
        results = compute_slo_report({"a": _m(100, 0)}, targets={"a": 0.95})
        assert len(results) == 1
        assert results[0].compliant is True

    def test_breach_when_below_target(self):
        results = compute_slo_report({"a": _m(100, 10)}, targets={"a": 0.99})
        assert results[0].compliant is False

    def test_wildcard_target_used_as_fallback(self):
        results = compute_slo_report({"a": _m(100, 0)}, targets={"*": 0.80})
        assert results[0].target == 0.80

    def test_default_target_is_099_when_no_targets(self):
        results = compute_slo_report({"a": _m(100, 0)})
        assert results[0].target == 0.99

    def test_no_records_is_not_compliant(self):
        results = compute_slo_report({"a": _m(0, 0)})
        # success_rate returns None for zero total
        assert results[0].actual is None
        assert results[0].compliant is False

    def test_results_sorted_alphabetically(self):
        metrics = {"b": _m(100, 0), "a": _m(100, 0)}
        results = compute_slo_report(metrics)
        assert [r.pipeline for r in results] == ["a", "b"]


# ---------------------------------------------------------------------------
# rank_by_gap
# ---------------------------------------------------------------------------

class TestRankByGap:
    def test_worst_gap_first(self):
        r1 = SLOResult("a", 0.99, 0.95, False)   # gap = 4pp
        r2 = SLOResult("b", 0.99, 0.98, False)   # gap = 1pp
        ranked = rank_by_gap([r2, r1])
        assert ranked[0].pipeline == "a"

    def test_none_gap_goes_last(self):
        r1 = SLOResult("a", 0.99, None, False)
        r2 = SLOResult("b", 0.99, 0.90, False)
        ranked = rank_by_gap([r1, r2])
        assert ranked[-1].pipeline == "a"
