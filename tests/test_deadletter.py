"""Tests for pipewatch.deadletter."""
from __future__ import annotations

import pytest
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.deadletter import (
    DeadLetterResult,
    _severity,
    compute_dead_letter,
    rank_by_dead_letter,
)


@dataclass
class _FakeMetric:
    total: int
    dead_letter: int


def _m(total: int, dead: int) -> _FakeMetric:
    return _FakeMetric(total=total, dead_letter=dead)


# ---------------------------------------------------------------------------
# _severity
# ---------------------------------------------------------------------------

class TestSeverity:
    def test_none_rate_is_no_data(self):
        assert _severity(None) == "no_data"

    def test_zero_rate_is_healthy(self):
        assert _severity(0.0) == "healthy"

    def test_below_warning_threshold_is_healthy(self):
        assert _severity(0.01) == "healthy"

    def test_above_warning_is_warning(self):
        assert _severity(0.05) == "warning"

    def test_above_critical_threshold_is_critical(self):
        assert _severity(0.15) == "critical"

    def test_exactly_critical_threshold_is_critical(self):
        # strictly greater than 0.10 → critical; 0.10 itself → warning
        assert _severity(0.10) == "warning"
        assert _severity(0.101) == "critical"


# ---------------------------------------------------------------------------
# compute_dead_letter
# ---------------------------------------------------------------------------

class TestComputeDeadLetter:
    def test_empty_records_returns_no_data(self):
        result = compute_dead_letter("pipe-a", [])
        assert result.severity == "no_data"
        assert result.rate is None
        assert result.total_processed == 0
        assert result.dead_letter_count == 0

    def test_all_successful_records(self):
        records = [_m(100, 0), _m(200, 0)]
        result = compute_dead_letter("pipe-b", records)
        assert result.rate == 0.0
        assert result.severity == "healthy"
        assert result.total_processed == 300

    def test_mixed_records_rate_computed(self):
        records = [_m(100, 5), _m(100, 5)]  # 10/200 = 0.05
        result = compute_dead_letter("pipe-c", records)
        assert result.rate == pytest.approx(0.05)
        assert result.dead_letter_count == 10
        assert result.severity == "warning"

    def test_high_dead_letter_is_critical(self):
        records = [_m(100, 20)]  # 20% dead-letter
        result = compute_dead_letter("pipe-d", records)
        assert result.severity == "critical"

    def test_zero_total_rate_is_none(self):
        records = [_m(0, 0)]
        result = compute_dead_letter("pipe-e", records)
        assert result.rate is None
        assert result.severity == "no_data"

    def test_str_contains_pipeline_name(self):
        records = [_m(100, 3)]
        result = compute_dead_letter("my-pipe", records)
        assert "my-pipe" in str(result)

    def test_str_no_data(self):
        result = compute_dead_letter("x", [])
        assert "no data" in str(result)


# ---------------------------------------------------------------------------
# rank_by_dead_letter
# ---------------------------------------------------------------------------

class TestRankByDeadLetter:
    def test_higher_rate_comes_first(self):
        r1 = DeadLetterResult("a", 100, 1, 0.01, "healthy")
        r2 = DeadLetterResult("b", 100, 20, 0.20, "critical")
        ranked = rank_by_dead_letter([r1, r2])
        assert ranked[0].pipeline == "b"

    def test_no_data_sorted_last(self):
        r1 = DeadLetterResult("a", 0, 0, None, "no_data")
        r2 = DeadLetterResult("b", 100, 5, 0.05, "warning")
        ranked = rank_by_dead_letter([r1, r2])
        assert ranked[-1].pipeline == "a"

    def test_empty_list_returns_empty(self):
        assert rank_by_dead_letter([]) == []
