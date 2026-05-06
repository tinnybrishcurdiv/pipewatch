"""CLI sub-command: pipewatch dependency — inspect pipeline dependency graphs."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.dependency import DependencyGraph, graph_from_dict


def build_dependency_parser(sub: "argparse._SubParsersAction | None" = None) -> argparse.ArgumentParser:  # noqa: E501
    kwargs = dict(description="Inspect pipeline dependency relationships.")
    parser = sub.add_parser("dependency", **kwargs) if sub else argparse.ArgumentParser(**kwargs)
    parser.add_argument("--config", default="dependency.json",
                        help="JSON file mapping pipeline -> [depends_on, ...]")
    parser.add_argument("--pipeline", default=None,
                        help="Focus pipeline (show upstream/downstream/impact).")
    parser.add_argument("--transitive", action="store_true",
                        help="Show transitive downstream instead of direct only.")
    return parser


def _load_graph(config_path: str) -> DependencyGraph:
    path = Path(config_path)
    if not path.exists():
        return DependencyGraph()
    with path.open() as fh:
        raw = json.load(fh)
    return graph_from_dict(raw)


def run_dependency_command(args: argparse.Namespace, out=sys.stdout) -> None:
    graph = _load_graph(args.config)

    if not args.pipeline:
        pipelines = graph.all_pipelines()
        if not pipelines:
            out.write("No pipelines registered in dependency graph.\n")
            return
        out.write("Registered pipelines:\n")
        for p in pipelines:
            impact = graph.impact_count(p)
            out.write(f"  {p}  (impact: {impact} downstream)\n")
        return

    name = args.pipeline
    ups = graph.upstream(name)
    if args.transitive:
        downs = graph.transitive_downstream(name)
        label = "transitive downstream"
    else:
        downs = graph.downstream(name)
        label = "direct downstream"

    out.write(f"Pipeline : {name}\n")
    out.write(f"Upstream : {', '.join(ups) if ups else '(none)'}\n")
    out.write(f"{label.capitalize()}: {', '.join(downs) if downs else '(none)'}\n")
    out.write(f"Impact count : {graph.impact_count(name)}\n")


def main() -> None:  # pragma: no cover
    parser = build_dependency_parser()
    run_dependency_command(parser.parse_args())


if __name__ == "__main__":  # pragma: no cover
    main()
