"""Retention policy: prune history files older than a configured age."""
from __future__ import annotations

import os
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional


@dataclass
class RetentionPolicy:
    max_age_days: int = 30
    max_records_per_pipeline: Optional[int] = None

    def __post_init__(self) -> None:
        if self.max_age_days < 1:
            raise ValueError("max_age_days must be >= 1")
        if self.max_records_per_pipeline is not None and self.max_records_per_pipeline < 1:
            raise ValueError("max_records_per_pipeline must be >= 1")

    @property
    def cutoff(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self.max_age_days)


def _parse_ts(ts: str) -> datetime:
    """Parse an ISO-8601 timestamp, attaching UTC if naive."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def prune_file(path: Path, policy: RetentionPolicy) -> int:
    """Remove records from *path* that violate *policy*.

    Returns the number of records removed.
    """
    if not path.exists():
        return 0

    lines = path.read_text(encoding="utf-8").splitlines()
    records: List[dict] = []
    for line in lines:
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    cutoff = policy.cutoff
    kept = [r for r in records if _parse_ts(r.get("timestamp", "1970-01-01")) >= cutoff]

    if policy.max_records_per_pipeline is not None:
        kept = kept[-policy.max_records_per_pipeline :]

    removed = len(records) - len(kept)
    if removed:
        path.write_text(
            "\n".join(json.dumps(r) for r in kept) + ("\n" if kept else ""),
            encoding="utf-8",
        )
    return removed


def prune_directory(directory: Path, policy: RetentionPolicy) -> dict:
    """Apply *policy* to every *.jsonl* file in *directory*.

    Returns a mapping of filename -> records_removed.
    """
    results: dict = {}
    for entry in sorted(directory.glob("*.jsonl")):
        removed = prune_file(entry, policy)
        results[entry.name] = removed
    return results
