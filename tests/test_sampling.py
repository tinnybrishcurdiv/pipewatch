"""Tests for pipewatch.sampling and pipewatch.sampling_config."""
from __future__ import annotations

import json
import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.sampling import SamplingPolicy, SampleResult, sample_records
from pipewatch.sampling_config import (
    default_sampling_policy,
    sampling_policy_from_dict,
    sampling_policy_from_json,
)


def _make_metrics(n: int, pipeline: str = "pipe-a") -> list[PipelineMetrics]:
    return [
        PipelineMetrics(pipeline=pipeline, processed=10, failed=0, latency_ms=5.0)
        for _ in range(n)
    ]


# ---------------------------------------------------------------------------
# SamplingPolicy validation
# ---------------------------------------------------------------------------

class TestSamplingPolicy:
    def test_defaults_ok(self):
        p = SamplingPolicy()
        assert p.base_rate == 1.0
        assert p.min_rate == 0.05

    def test_zero_base_rate_raises(self):
        with pytest.raises(ValueError, match="base_rate"):
            SamplingPolicy(base_rate=0.0)

    def test_min_rate_above_base_rate_raises(self):
        with pytest.raises(ValueError, match="min_rate"):
            SamplingPolicy(base_rate=0.1, min_rate=0.5)

    def test_zero_threshold_raises(self):
        with pytest.raises(ValueError, match="high_volume_threshold"):
            SamplingPolicy(high_volume_threshold=0)

    def test_effective_rate_below_threshold_is_base(self):
        p = SamplingPolicy(base_rate=0.8, high_volume_threshold=500)
        assert p.effective_rate(100) == 0.8

    def test_effective_rate_halved_above_threshold(self):
        p = SamplingPolicy(base_rate=1.0, min_rate=0.05, high_volume_threshold=100)
        rate = p.effective_rate(200)
        assert rate == 0.5

    def test_effective_rate_never_below_min(self):
        p = SamplingPolicy(base_rate=1.0, min_rate=0.1, high_volume_threshold=10)
        rate = p.effective_rate(10_000)
        assert rate >= 0.1


# ---------------------------------------------------------------------------
# sample_records
# ---------------------------------------------------------------------------

class TestSampleRecords:
    def test_full_rate_keeps_all(self):
        records = _make_metrics(50)
        policy = SamplingPolicy(base_rate=1.0, seed=42)
        result = sample_records("pipe-a", records, policy)
        assert result.sampled_count == 50
        assert result.original_count == 50

    def test_zero_records_returns_empty(self):
        policy = SamplingPolicy(seed=0)
        result = sample_records("pipe-x", [], policy)
        assert result.original_count == 0
        assert result.sampled_count == 0
        assert result.records == []

    def test_result_str_contains_pipeline(self):
        records = _make_metrics(10)
        policy = SamplingPolicy(base_rate=1.0, seed=1)
        result = sample_records("pipe-a", records, policy)
        assert "pipe-a" in str(result)

    def test_sampling_is_reproducible_with_seed(self):
        records = _make_metrics(200)
        policy = SamplingPolicy(base_rate=0.5, seed=99)
        r1 = sample_records("p", records, policy)
        r2 = sample_records("p", records, policy)
        assert r1.sampled_count == r2.sampled_count


# ---------------------------------------------------------------------------
# sampling_config
# ---------------------------------------------------------------------------

class TestSamplingConfig:
    def test_default_policy_base_rate(self):
        p = default_sampling_policy()
        assert p.base_rate == 1.0

    def test_from_dict_overrides_base_rate(self):
        p = sampling_policy_from_dict({"sampling": {"base_rate": 0.5}})
        assert p.base_rate == 0.5

    def test_from_dict_empty_uses_defaults(self):
        p = sampling_policy_from_dict({})
        assert p.min_rate == 0.05

    def test_from_json(self):
        raw = json.dumps({"sampling": {"base_rate": 0.25, "min_rate": 0.05}})
        p = sampling_policy_from_json(raw)
        assert p.base_rate == 0.25
