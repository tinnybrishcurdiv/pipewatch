"""Tests for pipewatch.correlation."""
from __future__ import annotations

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.correlation import (
    CorrelationResult,
    _pearson,
    _strength,
    compute_correlations,
)


def _m(pipeline: str, success: int, total: int) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline=pipeline,
        total=total,
        success=success,
        failure=total - success,
        throughput_per_sec=None,
        avg_latency_ms=None,
    )


# ---------------------------------------------------------------------------
# _pearson
# ---------------------------------------------------------------------------

class TestPearson:
    def test_perfect_positive(self):
        r = _pearson([1, 2, 3, 4], [2, 4, 6, 8])
        assert r == pytest.approx(1.0)

    def test_perfect_negative(self):
        r = _pearson([1, 2, 3, 4], [8, 6, 4, 2])
        assert r == pytest.approx(-1.0)

    def test_uncorrelated_returns_value(self):
        r = _pearson([1, 2, 3], [3, 1, 2])
        assert r is not None
        assert -1.0 <= r <= 1.0

    def test_single_point_returns_none(self):
        assert _pearson([1.0], [1.0]) is None

    def test_constant_series_returns_none(self):
        assert _pearson([1, 1, 1], [2, 3, 4]) is None


# ---------------------------------------------------------------------------
# _strength
# ---------------------------------------------------------------------------

class TestStrength:
    def test_strong(self):
        assert _strength(0.9) == "strong"
        assert _strength(-0.85) == "strong"

    def test_moderate(self):
        assert _strength(0.6) == "moderate"

    def test_weak(self):
        assert _strength(0.3) == "weak"

    def test_none(self):
        assert _strength(0.1) == "none"


# ---------------------------------------------------------------------------
# compute_correlations
# ---------------------------------------------------------------------------

class TestComputeCorrelations:
    def _history(self):
        return {
            "alpha": [_m("alpha", s, 10) for s in [8, 9, 7, 10, 6]],
            "beta":  [_m("beta",  s, 10) for s in [8, 9, 7, 10, 6]],  # identical
            "gamma": [_m("gamma", s, 10) for s in [2, 1, 3,  0, 4]],  # inverse
        }

    def test_returns_list(self):
        results = compute_correlations(self._history())
        assert isinstance(results, list)

    def test_identical_series_strong_positive(self):
        results = compute_correlations(self._history())
        pair = next(r for r in results if {r.pipeline_a, r.pipeline_b} == {"alpha", "beta"})
        assert pair.coefficient == pytest.approx(1.0)
        assert pair.strength == "strong"

    def test_inverse_series_strong_negative(self):
        results = compute_correlations(self._history())
        pair = next(r for r in results if "gamma" in {r.pipeline_a, r.pipeline_b}
                    and "alpha" in {r.pipeline_a, r.pipeline_b})
        assert pair.coefficient == pytest.approx(-1.0)

    def test_sorted_by_abs_coefficient_descending(self):
        results = compute_correlations(self._history())
        coeffs = [abs(r.coefficient) for r in results]
        assert coeffs == sorted(coeffs, reverse=True)

    def test_too_few_points_excluded(self):
        history = {
            "a": [_m("a", 8, 10), _m("a", 9, 10)],  # only 2 points
            "b": [_m("b", 8, 10), _m("b", 9, 10)],
        }
        results = compute_correlations(history, min_points=3)
        assert results == []

    def test_str_representation(self):
        r = CorrelationResult("a", "b", 0.95, "strong")
        assert "a" in str(r)
        assert "b" in str(r)
        assert "strong" in str(r)
