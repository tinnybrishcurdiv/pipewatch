"""CLI sub-command: pipewatch filter — list pipelines matching criteria."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.metrics import to_dict
from pipewatch.pipeline_filter import apply_filters
from pipewatch.snapshot import SnapshotManager


def build_filter_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # noqa: SLF001
    """Build (or attach) the argument parser for the *filter* sub-command."""
    kwargs = dict(
        description="List pipelines matching a name pattern and/or status.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    if parent is not None:
        parser = parent.add_parser("filter", **kwargs)
    else:
        parser = argparse.ArgumentParser(prog="pipewatch filter", **kwargs)

    parser.add_argument(
        "--pattern",
        metavar="GLOB",
        default=None,
        help="Shell-style glob to match pipeline names (e.g. 'ingest_*').",
    )
    parser.add_argument(
        "--status",
        metavar="STATUS",
        default=None,
        choices=["healthy", "degraded", "critical", "unknown"],
        help="Only show pipelines with this status.",
    )
    parser.add_argument(
        "--output",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table).",
    )
    return parser


def run_filter_command(
    args: argparse.Namespace,
    manager: SnapshotManager,
) -> int:
    """Execute the filter command.  Returns an exit code."""
    snapshots = manager.all_snapshots()
    metrics_map = {name: snap.collector.latest() for name, snap in snapshots.items()}
    # Drop pipelines with no data yet
    metrics_map = {k: v for k, v in metrics_map.items() if v is not None}

    try:
        filtered = apply_filters(metrics_map, pattern=args.pattern, status=args.status)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not filtered:
        print("No pipelines match the given filters.")
        return 0

    if args.output == "json":
        print(json.dumps({name: to_dict(m) for name, m in filtered.items()}, indent=2))
    else:
        header = f"{'PIPELINE':<30}  {'STATUS':<10}  {'RATE':>10}  {'SUCCESS':>8}"
        print(header)
        print("-" * len(header))
        from pipewatch.metrics import status_label, success_rate
        from pipewatch.display import format_rate
        for name, m in sorted(filtered.items()):
            sr = success_rate(m)
            sr_str = f"{sr:.1%}" if sr is not None else "N/A"
            print(f"{name:<30}  {status_label(m):<10}  {format_rate(m):>10}  {sr_str:>8}")

    return 0


def main() -> None:  # pragma: no cover
    parser = build_filter_parser()
    args = parser.parse_args()
    manager = SnapshotManager()
    sys.exit(run_filter_command(args, manager))


if __name__ == "__main__":  # pragma: no cover
    main()
