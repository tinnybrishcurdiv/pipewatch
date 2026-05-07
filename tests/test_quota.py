"""Tests for pipewatch.quota and pipewatch.quota_config."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.quota import (
    QuotaConfig,
    QuotaResult,
    compute_quota,
    rank_by_utilisation,
)
from pipewatch.quota_config import (
    default_quota_config,
    quota_config_from_dict,
    quota_config_from_json,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class _FakeRecord:
    timestamp: float
    records_processed: int
    success: bool = True


def _make_metrics(pipeline: str, records: List[_FakeRecord]) -> PipelineMetrics:
    m = PipelineMetrics(pipeline=pipeline)
    m.records = records  # type: ignore[assignment]
    return m


# ---------------------------------------------------------------------------
# QuotaConfig
# ---------------------------------------------------------------------------

class TestQuotaConfig:
    def test_defaults_ok(self):
        cfg = QuotaConfig()
        assert cfg.default_max_tps == 1000.0
        assert cfg.per_pipeline == {}

    def test_zero_default_raises(self):
        with pytest.raises(ValueError, match="default_max_tps"):
            QuotaConfig(default_max_tps=0)

    def test_negative_per_pipeline_raises(self):
        with pytest.raises(ValueError, match="quota for 'pipe'"):
            QuotaConfig(per_pipeline={"pipe": -5.0})

    def test_max_tps_for_returns_per_pipeline_override(self):
        cfg = QuotaConfig(default_max_tps=500.0, per_pipeline={"fast": 2000.0})
        assert cfg.max_tps_for("fast") == 2000.0
        assert cfg.max_tps_for("other") == 500.0


# ---------------------------------------------------------------------------
# compute_quota
# ---------------------------------------------------------------------------

class TestComputeQuota:
    def test_no_records_returns_none_tps(self):
        cfg = QuotaConfig(default_max_tps=100.0)
        m = _make_metrics("p", [])
        r = compute_quota(m, cfg)
        assert r.current_tps is None
        assert r.utilisation_pct is None
        assert not r.exceeded

    def test_exceeded_when_tps_above_max(self):
        cfg = QuotaConfig(default_max_tps=10.0)
        now = time.time()
        records = [
            _FakeRecord(timestamp=now, records_processed=100),
            _FakeRecord(timestamp=now + 1.0, records_processed=100),
        ]
        m = _make_metrics("p", records)
        r = compute_quota(m, cfg)
        assert r.exceeded
        assert r.utilisation_pct is not None and r.utilisation_pct > 100.0

    def test_not_exceeded_when_tps_below_max(self):
        cfg = QuotaConfig(default_max_tps=1000.0)
        now = time.time()
        records = [
            _FakeRecord(timestamp=now, records_processed=10),
            _FakeRecord(timestamp=now + 10.0, records_processed=10),
        ]
        m = _make_metrics("p", records)
        r = compute_quota(m, cfg)
        assert not r.exceeded

    def test_str_contains_pipeline_name(self):
        cfg = QuotaConfig()
        m = _make_metrics("my-pipe", [])
        assert "my-pipe" in str(compute_quota(m, cfg))


# ---------------------------------------------------------------------------
# rank_by_utilisation
# ---------------------------------------------------------------------------

def test_rank_by_utilisation_descending():
    results = [
        QuotaResult("a", 10.0, 100.0, False, 10.0),
        QuotaResult("b", 90.0, 100.0, False, 90.0),
        QuotaResult("c", None, 100.0, False, None),
    ]
    ranked = rank_by_utilisation(results)
    assert ranked[0].pipeline == "b"
    assert ranked[1].pipeline == "a"
    assert ranked[-1].pipeline == "c"


# ---------------------------------------------------------------------------
# quota_config_from_dict / quota_config_from_json
# ---------------------------------------------------------------------------

class TestQuotaConfigFromDict:
    def test_empty_dict_uses_defaults(self):
        cfg = quota_config_from_dict({})
        assert cfg.default_max_tps == 1000.0

    def test_custom_default(self):
        cfg = quota_config_from_dict({"default_max_tps": 250.0})
        assert cfg.default_max_tps == 250.0

    def test_per_pipeline_parsed(self):
        cfg = quota_config_from_dict({
            "per_pipeline": [{"pipeline": "etl", "max_tps": 50.0}]
        })
        assert cfg.max_tps_for("etl") == 50.0

    def test_from_json_string(self):
        cfg = quota_config_from_json('{"default_max_tps": 300}')
        assert cfg.default_max_tps == 300.0

    def test_default_quota_config(self):
        cfg = default_quota_config()
        assert isinstance(cfg, QuotaConfig)
