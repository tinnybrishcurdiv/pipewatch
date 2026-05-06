"""Pipeline dependency graph — track upstream/downstream relationships."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies (upstream -> downstream)."""

    _edges: Dict[str, Set[str]] = field(default_factory=dict, init=False)

    def add_dependency(self, pipeline: str, depends_on: str) -> None:
        """Register that *pipeline* depends on *depends_on*."""
        self._edges.setdefault(depends_on, set()).add(pipeline)
        # Ensure the pipeline itself has an entry so it shows up in queries.
        self._edges.setdefault(pipeline, set())

    def downstream(self, pipeline: str) -> List[str]:
        """Return all pipelines that directly depend on *pipeline*."""
        return sorted(self._edges.get(pipeline, set()))

    def upstream(self, pipeline: str) -> List[str]:
        """Return all pipelines that *pipeline* directly depends on."""
        return sorted(
            src for src, dsts in self._edges.items() if pipeline in dsts
        )

    def all_pipelines(self) -> List[str]:
        """Return every pipeline known to the graph."""
        return sorted(self._edges.keys())

    def transitive_downstream(self, pipeline: str) -> List[str]:
        """BFS over all transitive downstream dependents."""
        visited: Set[str] = set()
        queue = list(self.downstream(pipeline))
        while queue:
            node = queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            queue.extend(self.downstream(node))
        return sorted(visited)

    def impact_count(self, pipeline: str) -> int:
        """Number of pipelines transitively affected if *pipeline* fails."""
        return len(self.transitive_downstream(pipeline))


def graph_from_dict(raw: Dict[str, List[str]]) -> DependencyGraph:
    """Build a DependencyGraph from a mapping of pipeline -> list[depends_on]."""
    g = DependencyGraph()
    for pipeline, deps in raw.items():
        for dep in deps:
            g.add_dependency(pipeline, dep)
        g._edges.setdefault(pipeline, set())
    return g


def graph_from_json(json_str: str) -> DependencyGraph:
    import json
    return graph_from_dict(json.loads(json_str))
