"""Tests for pipewatch.budget and pipewatch.budget_config."""
from __future__ import annotations

import pytest

from pipewatch.budget import BudgetResult, compute_budget, rank_by_budget
from pipewatch.budget_config import (
    BudgetConfig,
    budget_config_from_dict,
    budget_config_from_json,
    default_budget_config,
)
from pipewatch.metrics import PipelineMetrics


def _m(total: int, failed: int) -> PipelineMetrics:
    return PipelineMetrics(
        total_records=total,
        failed_records=failed,
        processed_per_second=1.0,
        avg_latency_ms=10.0,
        last_seen=0.0,
    )


# ---------------------------------------------------------------------------
# compute_budget
# ---------------------------------------------------------------------------

class TestComputeBudget:
    def test_perfect_pipeline_full_budget(self):
        result = compute_budget("p", _m(1000, 0), slo_target=0.99)
        assert result.budget_remaining == 10
        assert result.actual_failures == 0
        assert result.burned_pct == 0.0

    def test_budget_exhausted_when_failures_exceed_allowed(self):
        # allowed = floor(0.01 * 100) = 1; actual = 5
        result = compute_budget("p", _m(100, 5), slo_target=0.99)
        assert result.budget_remaining == -4

    def test_burned_pct_none_when_allowed_is_zero(self):
        # 0.01 * 50 = 0.5 → floor = 0
        result = compute_budget("p", _m(50, 0), slo_target=0.99)
        assert result.allowed_failures == 0
        assert result.burned_pct is None

    def test_invalid_slo_raises(self):
        with pytest.raises(ValueError):
            compute_budget("p", _m(100, 0), slo_target=1.0)
        with pytest.raises(ValueError):
            compute_budget("p", _m(100, 0), slo_target=0.0)

    def test_str_contains_pipeline_name(self):
        r = compute_budget("my-pipe", _m(1000, 5), slo_target=0.99)
        assert "my-pipe" in str(r)

    def test_str_exhausted_label_when_over_budget(self):
        r = compute_budget("p", _m(100, 10), slo_target=0.99)
        assert "EXHAUSTED" in str(r)

    def test_str_ok_label_when_within_budget(self):
        r = compute_budget("p", _m(1000, 0), slo_target=0.99)
        assert "OK" in str(r)


# ---------------------------------------------------------------------------
# rank_by_budget
# ---------------------------------------------------------------------------

class TestRankByBudget:
    def test_worst_first(self):
        r1 = compute_budget("a", _m(1000, 0), slo_target=0.99)   # remaining=10
        r2 = compute_budget("b", _m(1000, 20), slo_target=0.99)  # remaining=-10
        ranked = rank_by_budget([r1, r2])
        assert ranked[0].pipeline == "b"

    def test_empty_list_returns_empty(self):
        assert rank_by_budget([]) == []


# ---------------------------------------------------------------------------
# BudgetConfig
# ---------------------------------------------------------------------------

class TestBudgetConfig:
    def test_default_config_ok(self):
        cfg = default_budget_config()
        assert cfg.default_slo == 0.99

    def test_slo_for_falls_back_to_default(self):
        cfg = BudgetConfig(default_slo=0.95)
        assert cfg.slo_for("unknown") == 0.95

    def test_slo_for_returns_override(self):
        cfg = BudgetConfig(default_slo=0.99, per_pipeline={"critical": 0.999})
        assert cfg.slo_for("critical") == 0.999

    def test_invalid_default_slo_raises(self):
        with pytest.raises(ValueError):
            BudgetConfig(default_slo=1.5)

    def test_from_dict_parses_correctly(self):
        cfg = budget_config_from_dict(
            {"default_slo": 0.95, "per_pipeline": {"pipe-a": 0.999}}
        )
        assert cfg.default_slo == 0.95
        assert cfg.per_pipeline["pipe-a"] == 0.999

    def test_from_json_round_trips(self):
        import json
        raw = json.dumps({"default_slo": 0.98})
        cfg = budget_config_from_json(raw)
        assert cfg.default_slo == 0.98
