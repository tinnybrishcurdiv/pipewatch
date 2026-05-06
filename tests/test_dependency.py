"""Tests for pipewatch.dependency and pipewatch.dependency_cli."""
from __future__ import annotations

import argparse
import io
import json
import textwrap
from pathlib import Path

import pytest

from pipewatch.dependency import DependencyGraph, graph_from_dict, graph_from_json
from pipewatch.dependency_cli import build_dependency_parser, run_dependency_command


# ---------------------------------------------------------------------------
# DependencyGraph unit tests
# ---------------------------------------------------------------------------

class TestDependencyGraph:
    def _graph(self) -> DependencyGraph:
        g = DependencyGraph()
        g.add_dependency("b", "a")  # b depends on a
        g.add_dependency("c", "b")  # c depends on b
        g.add_dependency("d", "b")  # d depends on b
        return g

    def test_downstream_direct(self):
        g = self._graph()
        assert g.downstream("a") == ["b"]
        assert g.downstream("b") == ["c", "d"]

    def test_downstream_leaf_is_empty(self):
        g = self._graph()
        assert g.downstream("c") == []

    def test_upstream(self):
        g = self._graph()
        assert g.upstream("b") == ["a"]
        assert g.upstream("c") == ["b"]

    def test_upstream_root_is_empty(self):
        g = self._graph()
        assert g.upstream("a") == []

    def test_transitive_downstream(self):
        g = self._graph()
        result = g.transitive_downstream("a")
        assert set(result) == {"b", "c", "d"}

    def test_impact_count(self):
        g = self._graph()
        assert g.impact_count("a") == 3
        assert g.impact_count("b") == 2
        assert g.impact_count("c") == 0

    def test_all_pipelines(self):
        g = self._graph()
        assert set(g.all_pipelines()) == {"a", "b", "c", "d"}

    def test_unknown_pipeline_returns_empty(self):
        g = self._graph()
        assert g.downstream("z") == []
        assert g.upstream("z") == []


def test_graph_from_dict():
    raw = {"b": ["a"], "c": ["b"]}
    g = graph_from_dict(raw)
    assert g.downstream("a") == ["b"]
    assert g.downstream("b") == ["c"]


def test_graph_from_json():
    payload = json.dumps({"b": ["a"]})
    g = graph_from_json(payload)
    assert g.downstream("a") == ["b"]


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------

def _args(config="dep.json", pipeline=None, transitive=False) -> argparse.Namespace:
    return argparse.Namespace(config=config, pipeline=pipeline, transitive=transitive)


def test_no_pipeline_lists_all(tmp_path):
    cfg = tmp_path / "dep.json"
    cfg.write_text(json.dumps({"b": ["a"], "c": ["b"]}))
    out = io.StringIO()
    run_dependency_command(_args(config=str(cfg)), out=out)
    text = out.getvalue()
    assert "a" in text
    assert "impact" in text


def test_missing_config_reports_no_pipelines(tmp_path):
    out = io.StringIO()
    run_dependency_command(_args(config=str(tmp_path / "missing.json")), out=out)
    assert "No pipelines" in out.getvalue()


def test_pipeline_shows_upstream_downstream(tmp_path):
    cfg = tmp_path / "dep.json"
    cfg.write_text(json.dumps({"b": ["a"], "c": ["b"]}))
    out = io.StringIO()
    run_dependency_command(_args(config=str(cfg), pipeline="b"), out=out)
    text = out.getvalue()
    assert "a" in text  # upstream
    assert "c" in text  # downstream


def test_transitive_flag(tmp_path):
    cfg = tmp_path / "dep.json"
    cfg.write_text(json.dumps({"b": ["a"], "c": ["b"], "d": ["c"]}))
    out = io.StringIO()
    run_dependency_command(_args(config=str(cfg), pipeline="a", transitive=True), out=out)
    text = out.getvalue()
    assert "transitive" in text.lower()
    assert "d" in text


def test_build_dependency_parser_defaults():
    parser = build_dependency_parser()
    args = parser.parse_args([])
    assert args.config == "dependency.json"
    assert args.pipeline is None
    assert args.transitive is False
