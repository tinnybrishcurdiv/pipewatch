"""Persistent history logging for pipeline metrics snapshots."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from typing import List, Optional

DEFAULT_HISTORY_PATH = ".pipewatch_history.jsonl"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class HistoryWriter:
    """Appends pipeline metric snapshots to a newline-delimited JSON log."""

    def __init__(self, path: str = DEFAULT_HISTORY_PATH) -> None:
        self.path = path

    def record(self, pipeline: str, metrics_dict: dict, ts: Optional[str] = None) -> None:
        """Append a single pipeline snapshot entry to the history file."""
        entry = {
            "timestamp": ts or _now_iso(),
            "pipeline": pipeline,
            **metrics_dict,
        }
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")

    def record_all(self, snapshot_dict: dict, ts: Optional[str] = None) -> None:
        """Record all pipelines from an export_json-style dict."""
        ts = ts or snapshot_dict.get("exported_at") or _now_iso()
        for pipeline, data in snapshot_dict.get("pipelines", {}).items():
            self.record(pipeline, data, ts=ts)


class HistoryReader:
    """Reads and queries the newline-delimited JSON history log."""

    def __init__(self, path: str = DEFAULT_HISTORY_PATH) -> None:
        self.path = path

    def read_all(self) -> List[dict]:
        """Return all entries in chronological order."""
        if not os.path.exists(self.path):
            return []
        entries = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries

    def read_pipeline(self, pipeline: str) -> List[dict]:
        """Return all entries for a specific pipeline."""
        return [e for e in self.read_all() if e.get("pipeline") == pipeline]

    def export_csv(self, dest: str) -> int:
        """Write all history entries to a CSV file. Returns row count written."""
        entries = self.read_all()
        if not entries:
            return 0
        fieldnames = list(entries[0].keys())
        with open(dest, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(entries)
        return len(entries)
