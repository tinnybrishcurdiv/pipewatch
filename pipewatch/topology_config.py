"""Configuration helpers for the topology feature."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class TopologyConfig:
    """Settings that control topology analysis."""

    graph_file: str = "topology.json"
    """Path to the dependency-graph JSON file."""

    top_n: int = 0
    """Number of top pipelines to show (0 = all)."""

    highlight_threshold: float = 0.75
    """Centrality score above which a pipeline is considered *critical*."""

    ignored_pipelines: List[str] = field(default_factory=list)
    """Pipelines to exclude from topology scoring."""

    def __post_init__(self) -> None:
        if not 0.0 <= self.highlight_threshold <= 1.0:
            raise ValueError(
                f"highlight_threshold must be in [0, 1], got {self.highlight_threshold}"
            )
        if self.top_n < 0:
            raise ValueError(f"top_n must be >= 0, got {self.top_n}")

    def is_critical(self, centrality: float) -> bool:
        """Return True when *centrality* meets or exceeds the highlight threshold."""
        return centrality >= self.highlight_threshold


def _parse_config(data: Dict) -> TopologyConfig:
    return TopologyConfig(
        graph_file=data.get("graph_file", "topology.json"),
        top_n=int(data.get("top_n", 0)),
        highlight_threshold=float(data.get("highlight_threshold", 0.75)),
        ignored_pipelines=list(data.get("ignored_pipelines", [])),
    )


def topology_config_from_dict(data: Dict) -> TopologyConfig:
    """Build a :class:`TopologyConfig` from a plain dictionary."""
    return _parse_config(data.get("topology", data))


def topology_config_from_json(path: str) -> TopologyConfig:
    """Load a :class:`TopologyConfig` from a JSON file."""
    raw = json.loads(Path(path).read_text())
    return topology_config_from_dict(raw)


def default_topology_config() -> TopologyConfig:
    """Return a :class:`TopologyConfig` with all defaults."""
    return TopologyConfig()
