"""CLI entry-point for the topology command."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.dependency import DependencyGraph
from pipewatch.topology import TopologyScore, compute_topology, rank_by_centrality


def build_topology_parser(sub: "argparse._SubParsersAction | None" = None) -> argparse.ArgumentParser:  # noqa: F821
    kwargs = dict(description="Show topology scores for pipelines in a dependency graph.")
    parser = sub.add_parser("topology", **kwargs) if sub else argparse.ArgumentParser(**kwargs)
    parser.add_argument(
        "--graph",
        required=True,
        metavar="FILE",
        help="JSON file produced by 'pipewatch dependency --export'.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        metavar="N",
        help="Show only the top-N pipelines by centrality (0 = all).",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit results as JSON.",
    )
    return parser


def _load_graph(path: str) -> DependencyGraph:
    data = json.loads(Path(path).read_text())
    g = DependencyGraph()
    for edge in data.get("edges", []):
        g.add_dependency(upstream=edge["upstream"], downstream=edge["downstream"])
    return g


def _render_table(scores: List[TopologyScore]) -> str:
    header = f"{'Pipeline':<30} {'FanIn':>6} {'FanOut':>7} {'Downstream':>11} {'Centrality':>11}"
    sep = "-" * len(header)
    rows = [header, sep]
    for s in scores:
        rows.append(
            f"{s.pipeline:<30} {s.fan_in:>6} {s.fan_out:>7} "
            f"{s.total_downstream:>11} {s.centrality:>10.2f}"
        )
    return "\n".join(rows)


def run_topology_command(args: argparse.Namespace) -> int:
    try:
        graph = _load_graph(args.graph)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    scores = rank_by_centrality(compute_topology(graph))
    if args.top > 0:
        scores = scores[: args.top]

    if args.as_json:
        print(json.dumps([vars(s) for s in scores], indent=2))
    else:
        print(_render_table(scores))
    return 0


def main() -> None:  # pragma: no cover
    parser = build_topology_parser()
    sys.exit(run_topology_command(parser.parse_args()))


if __name__ == "__main__":  # pragma: no cover
    main()
