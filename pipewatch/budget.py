"""Error budget tracking: how much failure capacity remains in a rolling window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class BudgetResult:
    pipeline: str
    slo_target: float          # e.g. 0.99 for 99 %
    window_records: int
    allowed_failures: int      # floor((1 - slo) * window_records)
    actual_failures: int
    budget_remaining: int      # allowed - actual  (can be negative)
    burned_pct: Optional[float]  # actual / allowed * 100, None when allowed==0

    def __str__(self) -> str:  # noqa: D105
        remaining = f"{self.budget_remaining:+d}"
        burned = (
            f"{self.burned_pct:.1f}%" if self.burned_pct is not None else "n/a"
        )
        status = "OK" if self.budget_remaining >= 0 else "EXHAUSTED"
        return (
            f"{self.pipeline}: budget {status} | "
            f"remaining={remaining} failures | burned={burned}"
        )


def compute_budget(
    pipeline: str,
    metrics: PipelineMetrics,
    slo_target: float = 0.99,
) -> BudgetResult:
    """Compute the error budget for *pipeline* given its current metrics."""
    if not 0.0 < slo_target < 1.0:
        raise ValueError("slo_target must be between 0 and 1 exclusive")

    total = metrics.total_records
    failures = metrics.failed_records
    allowed = int((1.0 - slo_target) * total)
    remaining = allowed - failures
    burned_pct: Optional[float] = None
    if allowed > 0:
        burned_pct = min(failures / allowed * 100.0, 999.9)

    return BudgetResult(
        pipeline=pipeline,
        slo_target=slo_target,
        window_records=total,
        allowed_failures=allowed,
        actual_failures=failures,
        budget_remaining=remaining,
        burned_pct=burned_pct,
    )


def rank_by_budget(
    results: list[BudgetResult],
) -> list[BudgetResult]:
    """Return results sorted worst-first (most exhausted budget first)."""
    return sorted(results, key=lambda r: r.budget_remaining)
