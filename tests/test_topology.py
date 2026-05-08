"""Tests for pipewatch.topology."""
from __future__ import annotations

import pytest

from pipewatch.dependency import DependencyGraph
from pipewatch.topology import (
    TopologyScore,
    _count_transitive,
    compute_topology,
    rank_by_centrality,
)


def _graph() -> DependencyGraph:
    """Build a simple graph: A -> B -> C, A -> D."""
    g = DependencyGraph()
    g.add_dependency(upstream="A", downstream="B")
    g.add_dependency(upstream="B", downstream="C")
    g.add_dependency(upstream="A", downstream="D")
    return g


class TestCountTransitive:
    def test_leaf_node_returns_zero(self):
        g = _graph()
        assert _count_transitive(g, "C") == 0

    def test_mid_node_returns_one(self):
        g = _graph()
        assert _count_transitive(g, "B") == 1

    def test_root_node_returns_three(self):
        g = _graph()
        # A -> B -> C and A -> D  => B, C, D
        assert _count_transitive(g, "A") == 3

    def test_isolated_node_returns_zero(self):
        g = DependencyGraph()
        g.add_dependency(upstream="X", downstream="Y")
        assert _count_transitive(g, "Y") == 0


class TestComputeTopology:
    def test_empty_graph_returns_empty(self):
        g = DependencyGraph()
        assert compute_topology(g) == []

    def test_returns_score_for_every_node(self):
        g = _graph()
        scores = compute_topology(g)
        names = {s.pipeline for s in scores}
        assert names == {"A", "B", "C", "D"}

    def test_root_has_highest_centrality(self):
        g = _graph()
        scores = {s.pipeline: s for s in compute_topology(g)}
        assert scores["A"].centrality == pytest.approx(1.0)

    def test_leaf_centrality_is_zero(self):
        g = _graph()
        scores = {s.pipeline: s for s in compute_topology(g)}
        assert scores["C"].centrality == pytest.approx(0.0)
        assert scores["D"].centrality == pytest.approx(0.0)

    def test_fan_out_counts_direct_children(self):
        g = _graph()
        scores = {s.pipeline: s for s in compute_topology(g)}
        assert scores["A"].fan_out == 2
        assert scores["B"].fan_out == 1

    def test_fan_in_counts_direct_parents(self):
        g = _graph()
        scores = {s.pipeline: s for s in compute_topology(g)}
        assert scores["B"].fan_in == 1
        assert scores["A"].fan_in == 0

    def test_explicit_pipelines_subset(self):
        g = _graph()
        scores = compute_topology(g, pipelines=["A", "C"])
        assert {s.pipeline for s in scores} == {"A", "C"}


class TestRankByCentrality:
    def test_sorted_descending(self):
        g = _graph()
        ranked = rank_by_centrality(compute_topology(g))
        centralities = [s.centrality for s in ranked]
        assert centralities == sorted(centralities, reverse=True)

    def test_first_is_root(self):
        g = _graph()
        ranked = rank_by_centrality(compute_topology(g))
        assert ranked[0].pipeline == "A"

    def test_empty_list_returns_empty(self):
        assert rank_by_centrality([]) == []
