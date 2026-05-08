"""Pipeline topology scoring — ranks pipelines by their structural importance
in the dependency graph (fan-out, fan-in, and centrality)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.dependency import DependencyGraph


@dataclass
class TopologyScore:
    pipeline: str
    fan_out: int          # number of direct downstream dependents
    fan_in: int           # number of direct upstream dependencies
    total_downstream: int # transitive downstream count
    centrality: float     # simple ratio: total_downstream / max_possible

    def __str__(self) -> str:  # pragma: no cover
        return (
            f"{self.pipeline}: fan_in={self.fan_in} fan_out={self.fan_out} "
            f"downstream={self.total_downstream} centrality={self.centrality:.2f}"
        )


def _count_transitive(graph: DependencyGraph, pipeline: str) -> int:
    """BFS count of all transitive downstream nodes."""
    visited: set = set()
    queue = list(graph.downstream(pipeline))
    while queue:
        node = queue.pop()
        if node in visited:
            continue
        visited.add(node)
        queue.extend(graph.downstream(node))
    return len(visited)


def compute_topology(
    graph: DependencyGraph,
    pipelines: Optional[List[str]] = None,
) -> List[TopologyScore]:
    """Return topology scores for every pipeline in *graph*.

    Args:
        graph: A populated :class:`DependencyGraph`.
        pipelines: Optional explicit list; defaults to ``graph.all_pipelines()``.

    Returns:
        List of :class:`TopologyScore`, unsorted.
    """
    nodes = list(pipelines or graph.all_pipelines())
    if not nodes:
        return []

    scores: List[TopologyScore] = []
    transitive_counts: Dict[str, int] = {n: _count_transitive(graph, n) for n in nodes}
    max_downstream = max(transitive_counts.values()) if transitive_counts else 1

    for node in nodes:
        fan_out = len(list(graph.downstream(node)))
        fan_in = len(list(graph.upstream(node)))
        td = transitive_counts[node]
        centrality = td / max_downstream if max_downstream > 0 else 0.0
        scores.append(TopologyScore(
            pipeline=node,
            fan_out=fan_out,
            fan_in=fan_in,
            total_downstream=td,
            centrality=centrality,
        ))
    return scores


def rank_by_centrality(scores: List[TopologyScore]) -> List[TopologyScore]:
    """Return *scores* sorted descending by centrality then pipeline name."""
    return sorted(scores, key=lambda s: (-s.centrality, s.pipeline))
