"""Tests for pipewatch.pipeline_filter."""

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_filter import (
    apply_filters,
    filter_by_pattern,
    filter_by_status,
)


def _make_metrics(total: int, failed: int) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name="p",
        total_records=total,
        failed_records=failed,
        records_per_second=1.0,
        last_seen=0.0,
    )


# ---------------------------------------------------------------------------
# filter_by_pattern
# ---------------------------------------------------------------------------

class TestFilterByPattern:
    def test_exact_match(self):
        assert filter_by_pattern(["foo", "bar", "baz"], "foo") == ["foo"]

    def test_wildcard_suffix(self):
        result = filter_by_pattern(["ingest_a", "ingest_b", "export_c"], "ingest_*")
        assert result == ["ingest_a", "ingest_b"]

    def test_no_match_returns_empty(self):
        assert filter_by_pattern(["foo", "bar"], "zzz*") == []

    def test_star_matches_all(self):
        names = ["alpha", "beta", "gamma"]
        assert filter_by_pattern(names, "*") == names


# ---------------------------------------------------------------------------
# filter_by_status
# ---------------------------------------------------------------------------

class TestFilterByStatus:
    def _map(self):
        return {
            "healthy_pipe": _make_metrics(100, 0),    # 100 % -> healthy
            "degraded_pipe": _make_metrics(100, 10),  # 90 % -> degraded
            "critical_pipe": _make_metrics(100, 60),  # 40 % -> critical
        }

    def test_healthy_filter(self):
        result = filter_by_status(self._map(), "healthy")
        assert result == ["healthy_pipe"]

    def test_critical_filter(self):
        result = filter_by_status(self._map(), "critical")
        assert result == ["critical_pipe"]

    def test_case_insensitive(self):
        result = filter_by_status(self._map(), "HEALTHY")
        assert result == ["healthy_pipe"]

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Unknown status"):
            filter_by_status(self._map(), "superb")

    def test_no_match_returns_empty(self):
        result = filter_by_status(self._map(), "unknown")
        assert result == []


# ---------------------------------------------------------------------------
# apply_filters
# ---------------------------------------------------------------------------

class TestApplyFilters:
    def _map(self):
        return {
            "ingest_ok": _make_metrics(100, 0),
            "ingest_bad": _make_metrics(100, 60),
            "export_ok": _make_metrics(100, 0),
        }

    def test_no_filters_returns_all(self):
        m = self._map()
        assert apply_filters(m) == m

    def test_pattern_only(self):
        result = apply_filters(self._map(), pattern="ingest_*")
        assert set(result.keys()) == {"ingest_ok", "ingest_bad"}

    def test_status_only(self):
        result = apply_filters(self._map(), status="healthy")
        assert set(result.keys()) == {"ingest_ok", "export_ok"}

    def test_pattern_and_status(self):
        result = apply_filters(self._map(), pattern="ingest_*", status="healthy")
        assert set(result.keys()) == {"ingest_ok"}

    def test_combined_no_match(self):
        result = apply_filters(self._map(), pattern="export_*", status="critical")
        assert result == {}
