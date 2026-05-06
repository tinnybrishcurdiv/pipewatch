"""Tests for pipewatch.capacity and pipewatch.capacity_config."""
from __future__ import annotations

import pytest

from pipewatch.capacity import CapacityResult, compute_capacity, rank_by_headroom
from pipewatch.capacity_config import (
    CapacityConfig,
    capacity_config_from_dict,
    default_capacity_config,
)
from pipewatch.metrics import PipelineMetrics


def _m(throughputs: list[float]) -> PipelineMetrics:
    from pipewatch.metrics import PipelineMetrics
    from unittest.mock import MagicMock

    records = []
    for t in throughputs:
        r = MagicMock()
        r.throughput = t
        records.append(r)
    m = MagicMock(spec=PipelineMetrics)
    m.records = records
    return m


class TestComputeCapacity:
    def test_empty_records_returns_none_tps(self):
        m = _m([])
        result = compute_capacity("pipe", m)
        assert result.current_tps is None
        assert result.peak_tps is None

    def test_single_record_tps(self):
        m = _m([10.0])
        result = compute_capacity("pipe", m)
        assert result.current_tps == pytest.approx(10.0)
        assert result.peak_tps == pytest.approx(10.0)

    def test_average_tps_computed(self):
        m = _m([4.0, 6.0, 8.0])
        result = compute_capacity("pipe", m)
        assert result.current_tps == pytest.approx(6.0)

    def test_peak_tps_is_max(self):
        m = _m([1.0, 5.0, 3.0])
        result = compute_capacity("pipe", m)
        assert result.peak_tps == pytest.approx(5.0)

    def test_headroom_none_when_no_capacity(self):
        m = _m([10.0])
        result = compute_capacity("pipe", m, peak_capacity=None)
        assert result.headroom_pct is None
        assert not result.at_risk

    def test_headroom_calculated_correctly(self):
        m = _m([60.0])
        result = compute_capacity("pipe", m, peak_capacity=100.0)
        assert result.headroom_pct == pytest.approx(40.0)

    def test_at_risk_when_above_threshold(self):
        m = _m([85.0])
        result = compute_capacity("pipe", m, peak_capacity=100.0, at_risk_threshold=80.0)
        assert result.at_risk is True

    def test_not_at_risk_when_below_threshold(self):
        m = _m([70.0])
        result = compute_capacity("pipe", m, peak_capacity=100.0, at_risk_threshold=80.0)
        assert result.at_risk is False

    def test_str_contains_pipeline_name(self):
        m = _m([50.0])
        result = compute_capacity("my-pipe", m, peak_capacity=100.0)
        assert "my-pipe" in str(result)

    def test_str_at_risk_label(self):
        m = _m([95.0])
        result = compute_capacity("p", m, peak_capacity=100.0)
        assert "AT RISK" in str(result)


class TestRankByHeadroom:
    def test_most_at_risk_first(self):
        r1 = CapacityResult("a", 10.0, 10.0, 90.0, 100.0)
        r2 = CapacityResult("b", 80.0, 80.0, 20.0, 100.0)
        ranked = rank_by_headroom([r1, r2])
        assert ranked[0].pipeline == "b"

    def test_unknown_headroom_last(self):
        r1 = CapacityResult("a", None, None, None, None)
        r2 = CapacityResult("b", 5.0, 5.0, 50.0, 10.0)
        ranked = rank_by_headroom([r1, r2])
        assert ranked[-1].pipeline == "a"


class TestCapacityConfig:
    def test_default_config_ok(self):
        cfg = default_capacity_config()
        assert cfg.at_risk_threshold == 80.0
        assert cfg.default_peak_capacity is None

    def test_invalid_threshold_raises(self):
        with pytest.raises(ValueError):
            CapacityConfig(at_risk_threshold=0.0)

    def test_per_pipeline_override(self):
        cfg = capacity_config_from_dict(
            {"default_peak_capacity": 100, "per_pipeline": {"special": 500}}
        )
        assert cfg.peak_capacity_for("special") == 500.0
        assert cfg.peak_capacity_for("other") == 100.0

    def test_missing_pipeline_uses_default(self):
        cfg = capacity_config_from_dict({"default_peak_capacity": 200})
        assert cfg.peak_capacity_for("any") == 200.0
