"""CLI entry-point for displaying a one-shot aggregated pipeline summary."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from pipewatch.aggregator import aggregate
from pipewatch.exporter import export_json
from pipewatch.snapshot import SnapshotManager
from pipewatch.summary_display import render_summary


def build_summary_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-summary",
        description="Print an aggregated summary of all tracked pipelines.",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=60,
        metavar="SECONDS",
        help="Metrics window in seconds (default: 60).",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Output summary as JSON instead of a table.",
    )
    return parser


def run_summary_command(
    manager: SnapshotManager,
    window: int = 60,
    as_json: bool = False,
    out=None,
) -> None:
    if out is None:
        out = sys.stdout

    metrics_map = {
        name: snap.collector.latest(window=window)
        for name, snap in manager.snapshots().items()
    }
    summary = aggregate(metrics_map)

    if as_json:
        data = {
            "total_pipelines": summary.total_pipelines,
            "healthy": summary.healthy,
            "degraded": summary.degraded,
            "failing": summary.failing,
            "total_processed": summary.total_processed,
            "total_errors": summary.total_errors,
            "avg_success_rate": summary.avg_success_rate,
            "overall_success_rate": summary.overall_success_rate,
            "slowest_pipeline": summary.slowest_pipeline,
            "fastest_pipeline": summary.fastest_pipeline,
        }
        out.write(json.dumps(data, indent=2) + "\n")
    else:
        out.write(render_summary(summary) + "\n")


def main(argv: Optional[list] = None) -> None:  # pragma: no cover
    parser = build_summary_parser()
    args = parser.parse_args(argv)
    manager = SnapshotManager()
    run_summary_command(manager, window=args.window, as_json=args.as_json)


if __name__ == "__main__":  # pragma: no cover
    main()
