"""Adaptive sampling: reduce record volume while preserving statistical fidelity."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Iterable, List

from pipewatch.metrics import PipelineMetrics


@dataclass
class SamplingPolicy:
    """Controls how aggressively records are sampled per pipeline."""

    base_rate: float = 1.0          # fraction of records to keep (0 < rate <= 1)
    min_rate: float = 0.05          # floor — never drop below this rate
    high_volume_threshold: int = 500  # records/window above which rate is halved
    seed: int | None = None

    def __post_init__(self) -> None:
        if not (0 < self.base_rate <= 1.0):
            raise ValueError("base_rate must be in (0, 1]")
        if not (0 < self.min_rate <= self.base_rate):
            raise ValueError("min_rate must be in (0, base_rate]")
        if self.high_volume_threshold < 1:
            raise ValueError("high_volume_threshold must be >= 1")

    def effective_rate(self, total_records: int) -> float:
        """Return the sampling rate adjusted for current record volume."""
        rate = self.base_rate
        volume = total_records
        while volume >= self.high_volume_threshold:
            rate = max(rate / 2.0, self.min_rate)
            volume //= 2
        return rate


@dataclass
class SampleResult:
    pipeline: str
    original_count: int
    sampled_count: int
    effective_rate: float
    records: List[PipelineMetrics] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: kept {self.sampled_count}/{self.original_count} "
            f"records ({self.effective_rate:.0%} sample rate)"
        )


def sample_records(
    pipeline: str,
    records: Iterable[PipelineMetrics],
    policy: SamplingPolicy,
) -> SampleResult:
    """Apply adaptive sampling to *records* for a single pipeline."""
    rng = random.Random(policy.seed)
    all_records: List[PipelineMetrics] = list(records)
    rate = policy.effective_rate(len(all_records))
    kept = [r for r in all_records if rng.random() < rate]
    return SampleResult(
        pipeline=pipeline,
        original_count=len(all_records),
        sampled_count=len(kept),
        effective_rate=rate,
        records=kept,
    )
