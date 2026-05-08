"""Tests for pipewatch.drift."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.drift import DriftResult, compute_drift
from pipewatch.metrics import PipelineMetrics


def _m(
    name: str,
    successes: int = 10,
    failures: int = 0,
    count: int = 10,
    duration: float = 1.0,
) -> PipelineMetrics:
    record = MagicMock()
    record.count = count
    record.duration_seconds = duration
    m = MagicMock(spec=PipelineMetrics)
    m.name = name
    m.records = [record] * (successes + failures)
    # Wire success_rate helper via the real function path
    m.successes = successes
    m.failures = failures
    m.total = successes + failures
    return m


def _real_m(name: str, successes: int, failures: int) -> PipelineMetrics:
    """Return a real PipelineMetrics-like object using MagicMock with real sr."""
    from pipewatch.metrics import PipelineMetrics as _PM  # noqa: F401
    obj = MagicMock(spec=_PM)
    obj.name = name
    obj.records = []
    # Patch success_rate at module level via monkeypatch is complex; use a
    # simpler approach: real PipelineMetrics dataclass if available.
    return obj


class TestDriftResult:
    def test_str_drifted(self):
        r = DriftResult(
            pipeline="pipe-a",
            success_rate_before=0.9,
            success_rate_after=0.7,
            throughput_before=None,
            throughput_after=None,
            drifted=True,
            reason="success_rate Δ=-20.0%",
        )
        s = str(r)
        assert "DRIFTED" in s
        assert "pipe-a" in s

    def test_str_stable(self):
        r = DriftResult(
            pipeline="pipe-b",
            success_rate_before=0.95,
            success_rate_after=0.96,
            throughput_before=None,
            throughput_after=None,
            drifted=False,
            reason="no significant change",
        )
        assert "stable" in str(r)

    def test_str_none_rates(self):
        r = DriftResult(
            pipeline="pipe-c",
            success_rate_before=None,
            success_rate_after=None,
            throughput_before=None,
            throughput_after=None,
            drifted=True,
            reason="pipeline appeared in after snapshot",
        )
        s = str(r)
        assert "pipe-c" in s


class TestComputeDrift:
    def _metrics_with_sr(self, name: str, sr: float) -> PipelineMetrics:
        """Build a mock where success_rate() will return sr."""
        from pipewatch import metrics as _metrics_mod
        obj = MagicMock(spec=PipelineMetrics)
        obj.name = name
        # records list with controlled success/failure counts
        total = 100
        succ = int(sr * total)
        fail = total - succ
        rec = MagicMock()
        rec.count = 1
        rec.duration_seconds = 0.1
        success_records = [MagicMock(success=True, count=1, duration_seconds=0.1)] * succ
        fail_records = [MagicMock(success=False, count=1, duration_seconds=0.1)] * fail
        obj.records = success_records + fail_records
        return obj

    def test_empty_snapshots_returns_empty(self):
        assert compute_drift({}, {}) == []

    def test_pipeline_only_in_after_is_drifted(self):
        after_m = MagicMock(spec=PipelineMetrics)
        after_m.records = []
        results = compute_drift({}, {"new-pipe": after_m})
        assert len(results) == 1
        r = results[0]
        assert r.pipeline == "new-pipe"
        assert r.drifted is True
        assert "appeared" in r.reason

    def test_pipeline_only_in_before_is_drifted(self):
        before_m = MagicMock(spec=PipelineMetrics)
        before_m.records = []
        results = compute_drift({"old-pipe": before_m}, {})
        assert len(results) == 1
        assert results[0].drifted is True
        assert "missing" in results[0].reason

    def test_stable_pipeline_not_drifted(self):
        rec = MagicMock(count=1, duration_seconds=1.0)
        m1 = MagicMock(spec=PipelineMetrics)
        m1.records = [rec] * 10
        m2 = MagicMock(spec=PipelineMetrics)
        m2.records = [rec] * 10
        results = compute_drift({"p": m1}, {"p": m2}, sr_threshold=0.05)
        assert len(results) == 1
        # No success_rate difference since both have same mock records
        assert results[0].drifted is False

    def test_results_sorted_by_pipeline_name(self):
        rec = MagicMock(count=1, duration_seconds=1.0)
        mk = lambda: MagicMock(spec=PipelineMetrics, records=[rec])
        before = {"zebra": mk(), "alpha": mk()}
        after = {"zebra": mk(), "alpha": mk()}
        results = compute_drift(before, after)
        names = [r.pipeline for r in results]
        assert names == sorted(names)
