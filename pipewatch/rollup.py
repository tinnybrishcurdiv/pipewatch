"""Periodic rollup: collapse per-minute history records into hourly summaries."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


@dataclass
class RollupBucket:
    """Aggregated statistics for one pipeline over one time bucket."""

    pipeline: str
    bucket_start: str          # ISO-8601 UTC
    record_count: int
    avg_success_rate: Optional[float]
    avg_throughput: Optional[float]
    min_success_rate: Optional[float]
    max_success_rate: Optional[float]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "bucket_start": self.bucket_start,
            "record_count": self.record_count,
            "avg_success_rate": self.avg_success_rate,
            "avg_throughput": self.avg_throughput,
            "min_success_rate": self.min_success_rate,
            "max_success_rate": self.max_success_rate,
        }


def _truncate_to_hour(iso: str) -> str:
    """Return ISO string truncated to the start of the hour."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    truncated = dt.replace(minute=0, second=0, microsecond=0)
    return truncated.isoformat()


def rollup_records(records: List[dict], bucket_fn=_truncate_to_hour) -> List[RollupBucket]:
    """Group *records* (dicts from history NDJSON) into hourly RollupBuckets.

    Each record must contain at least ``pipeline``, ``recorded_at``,
    ``success_rate``, and ``throughput`` keys.
    """
    from collections import defaultdict

    groups: dict[tuple, list] = defaultdict(list)
    for rec in records:
        pipeline = rec.get("pipeline", "unknown")
        ts = rec.get("recorded_at", "")
        bucket = bucket_fn(ts) if ts else ""
        groups[(pipeline, bucket)].append(rec)

    buckets: List[RollupBucket] = []
    for (pipeline, bucket_start), recs in sorted(groups.items()):
        rates = [r["success_rate"] for r in recs if r.get("success_rate") is not None]
        throughputs = [r["throughput"] for r in recs if r.get("throughput") is not None]

        buckets.append(
            RollupBucket(
                pipeline=pipeline,
                bucket_start=bucket_start,
                record_count=len(recs),
                avg_success_rate=sum(rates) / len(rates) if rates else None,
                avg_throughput=sum(throughputs) / len(throughputs) if throughputs else None,
                min_success_rate=min(rates) if rates else None,
                max_success_rate=max(rates) if rates else None,
            )
        )
    return buckets


def rollup_file(src: Path, dst: Path) -> int:
    """Read NDJSON from *src*, roll up, and append buckets to *dst*.

    Returns the number of buckets written.
    """
    records = []
    with src.open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    buckets = rollup_records(records)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("a") as fh:
        for b in buckets:
            fh.write(json.dumps(b.to_dict()) + "\n")
    return len(buckets)
