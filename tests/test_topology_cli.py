"""Tests for pipewatch.topology_cli."""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from pipewatch.topology_cli import (
    _load_graph,
    _render_table,
    build_topology_parser,
    run_topology_command,
)
from pipewatch.topology import compute_topology, rank_by_centrality


def _write_graph(tmp_path: Path, edges: list) -> str:
    p = tmp_path / "graph.json"
    p.write_text(json.dumps({"edges": edges}))
    return str(p)


class TestBuildTopologyParser:
    def test_defaults(self, tmp_path):
        path = _write_graph(tmp_path, [])
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path])
        assert args.top == 0
        assert args.as_json is False

    def test_custom_top(self, tmp_path):
        path = _write_graph(tmp_path, [])
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path, "--top", "3"])
        assert args.top == 3

    def test_json_flag(self, tmp_path):
        path = _write_graph(tmp_path, [])
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path, "--json"])
        assert args.as_json is True


class TestLoadGraph:
    def test_loads_edges(self, tmp_path):
        path = _write_graph(tmp_path, [{"upstream": "A", "downstream": "B"}])
        g = _load_graph(path)
        assert "B" in list(g.downstream("A"))

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            _load_graph("/nonexistent/path.json")

    def test_invalid_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not json")
        with pytest.raises(json.JSONDecodeError):
            _load_graph(str(bad))


class TestRunTopologyCommand:
    def test_returns_zero_on_success(self, tmp_path):
        path = _write_graph(tmp_path, [{"upstream": "A", "downstream": "B"}])
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path])
        assert run_topology_command(args) == 0

    def test_returns_one_on_bad_file(self, tmp_path):
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", str(tmp_path / "nope.json")])
        assert run_topology_command(args) == 1

    def test_json_output_is_valid(self, tmp_path, capsys):
        path = _write_graph(tmp_path, [{"upstream": "X", "downstream": "Y"}])
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path, "--json"])
        run_topology_command(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert isinstance(data, list)
        assert all("pipeline" in item for item in data)

    def test_top_limits_output(self, tmp_path, capsys):
        edges = [
            {"upstream": "A", "downstream": "B"},
            {"upstream": "A", "downstream": "C"},
            {"upstream": "B", "downstream": "D"},
        ]
        path = _write_graph(tmp_path, edges)
        parser = build_topology_parser()
        args = parser.parse_args(["--graph", path, "--json", "--top", "2"])
        run_topology_command(args)
        data = json.loads(capsys.readouterr().out)
        assert len(data) == 2


class TestRenderTable:
    def test_contains_pipeline_name(self):
        from pipewatch.topology import TopologyScore
        scores = [TopologyScore(pipeline="my-pipe", fan_out=2, fan_in=0, total_downstream=5, centrality=1.0)]
        table = _render_table(scores)
        assert "my-pipe" in table

    def test_contains_header(self):
        table = _render_table([])
        assert "Pipeline" in table
        assert "Centrality" in table
