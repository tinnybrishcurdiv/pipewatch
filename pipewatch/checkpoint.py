"""Checkpoint management: save and compare pipeline processing offsets."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class CheckpointEntry:
    pipeline: str
    offset: int
    recorded_at: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "offset": self.offset,
            "recorded_at": self.recorded_at,
        }


@dataclass
class CheckpointDiff:
    pipeline: str
    previous: Optional[int]
    current: int

    @property
    def delta(self) -> Optional[int]:
        if self.previous is None:
            return None
        return self.current - self.previous

    def __str__(self) -> str:
        delta_str = f"+{self.delta}" if self.delta is not None and self.delta >= 0 else str(self.delta)
        prev_str = str(self.previous) if self.previous is not None else "n/a"
        return (
            f"{self.pipeline}: offset {prev_str} -> {self.current}"
            + (f" (delta={delta_str})" if self.delta is not None else " (new)")
        )


@dataclass
class CheckpointStore:
    path: str
    _data: Dict[str, int] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            self._data = {entry["pipeline"]: entry["offset"] for entry in raw}

    def get(self, pipeline: str) -> Optional[int]:
        return self._data.get(pipeline)

    def update(self, pipeline: str, offset: int) -> CheckpointDiff:
        previous = self._data.get(pipeline)
        self._data[pipeline] = offset
        return CheckpointDiff(pipeline=pipeline, previous=previous, current=offset)

    def save(self, recorded_at: Optional[str] = None) -> None:
        from pipewatch.history import _now_iso  # local import to avoid cycles
        ts = recorded_at or _now_iso()
        entries = [
            CheckpointEntry(pipeline=p, offset=o, recorded_at=ts).to_dict()
            for p, o in sorted(self._data.items())
        ]
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(entries, fh, indent=2)

    def all_pipelines(self) -> list:
        return sorted(self._data.keys())
