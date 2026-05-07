"""Tests for pipewatch.staleness."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.staleness import StalenessResult, compute_staleness, rank_by_staleness


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_record(seconds_ago: float):
    """Minimal fake record with a timestamp attribute."""
    class _Rec:
        timestamp = _NOW - timedelta(seconds=seconds_ago)
    return _Rec()


def _m(pipeline: str, *ages: float) -> PipelineMetrics:
    """Build a PipelineMetrics stub with records at given ages (seconds before _NOW)."""
    records = [_make_record(a) for a in ages]
    return PipelineMetrics(pipeline=pipeline, records=records)


# ---------------------------------------------------------------------------
# compute_staleness
# ---------------------------------------------------------------------------

class TestComputeStaleness:
    def test_no_records_is_stale(self):
        m = _m("pipe-a")
        result = compute_staleness(m, threshold_seconds=60.0, now=_NOW)
        assert result.is_stale is True
        assert result.last_seen is None
        assert result.age_seconds is None

    def test_recent_record_not_stale(self):
        m = _m("pipe-b", 10.0)
        result = compute_staleness(m, threshold_seconds=60.0, now=_NOW)
        assert result.is_stale is False
        assert result.age_seconds == pytest.approx(10.0)

    def test_old_record_is_stale(self):
        m = _m("pipe-c", 400.0)
        result = compute_staleness(m, threshold_seconds=300.0, now=_NOW)
        assert result.is_stale is True
        assert result.age_seconds == pytest.approx(400.0)

    def test_exact_threshold_not_stale(self):
        m = _m("pipe-d", 300.0)
        result = compute_staleness(m, threshold_seconds=300.0, now=_NOW)
        # age == threshold → not stale (strictly greater)
        assert result.is_stale is False

    def test_multiple_records_uses_most_recent(self):
        m = _m("pipe-e", 500.0, 50.0, 200.0)
        result = compute_staleness(m, threshold_seconds=100.0, now=_NOW)
        assert result.age_seconds == pytest.approx(50.0)
        assert result.is_stale is False

    def test_pipeline_name_preserved(self):
        m = _m("my-pipeline", 10.0)
        result = compute_staleness(m, threshold_seconds=60.0, now=_NOW)
        assert result.pipeline == "my-pipeline"

    def test_zero_threshold_raises(self):
        m = _m("pipe-f", 10.0)
        with pytest.raises(ValueError):
            compute_staleness(m, threshold_seconds=0.0, now=_NOW)

    def test_negative_threshold_raises(self):
        m = _m("pipe-g", 10.0)
        with pytest.raises(ValueError):
            compute_staleness(m, threshold_seconds=-5.0, now=_NOW)

    def test_str_contains_pipeline_and_flag(self):
        m = _m("pipe-h", 400.0)
        result = compute_staleness(m, threshold_seconds=300.0, now=_NOW)
        s = str(result)
        assert "pipe-h" in s
        assert "STALE" in s

    def test_str_ok_when_not_stale(self):
        m = _m("pipe-i", 10.0)
        result = compute_staleness(m, threshold_seconds=300.0, now=_NOW)
        assert "ok" in str(result)


# ---------------------------------------------------------------------------
# rank_by_staleness
# ---------------------------------------------------------------------------

class TestRankByStaleness:
    def _result(self, pipeline, age, threshold=300.0):
        is_stale = (age is None) or (age > threshold)
        return StalenessResult(
            pipeline=pipeline,
            last_seen=None if age is None else _NOW - timedelta(seconds=age),
            age_seconds=age,
            threshold_seconds=threshold,
            is_stale=is_stale,
        )

    def test_stale_before_healthy(self):
        results = [
            self._result("healthy", 10.0),
            self._result("stale", 400.0),
        ]
        ranked = rank_by_staleness(results)
        assert ranked[0].pipeline == "stale"

    def test_older_stale_first_among_stale(self):
        results = [
            self._result("less-stale", 310.0),
            self._result("more-stale", 600.0),
        ]
        ranked = rank_by_staleness(results)
        assert ranked[0].pipeline == "more-stale"

    def test_never_seen_sorted_last_among_stale(self):
        results = [
            self._result("never", None),
            self._result("old", 500.0),
        ]
        ranked = rank_by_staleness(results)
        # both stale; old has positive age so sorts before never (age=-inf)
        assert ranked[0].pipeline == "old"
