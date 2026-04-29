"""CLI command for displaying pipeline trend analysis."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.metrics import PipelineMetrics
from pipewatch.trend import TrendResult, compute_trend, rank_by_trend


def build_trend_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    kwargs = dict(description="Show success-rate trend for each pipeline from a JSONL history file.")
    parser = parent.add_parser("trend", **kwargs) if parent else argparse.ArgumentParser(**kwargs)  # type: ignore[arg-type]
    parser.add_argument("history_file", type=Path, help="Path to JSONL history file produced by HistoryWriter")
    parser.add_argument("--min-points", type=int, default=2, metavar="N", help="Minimum snapshots required (default: 2)")
    parser.add_argument("--threshold", type=float, default=2.0, metavar="PP", help="Min percentage-point change for non-stable (default: 2.0)")
    parser.add_argument("--pipeline", metavar="NAME", help="Filter to a single pipeline")
    return parser


def _load_history(path: Path) -> dict[str, list[PipelineMetrics]]:
    """Read a JSONL file and group PipelineMetrics by pipeline name."""
    grouped: dict[str, list[PipelineMetrics]] = {}
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            name = record["pipeline_name"]
            m = PipelineMetrics(
                pipeline_name=name,
                success_count=record.get("success_count", 0),
                failure_count=record.get("failure_count", 0),
                avg_latency_ms=record.get("avg_latency_ms"),
                throughput=record.get("throughput"),
                window_seconds=record.get("window_seconds", 60),
            )
            grouped.setdefault(name, []).append(m)
    return grouped


def run_trend_command(args: argparse.Namespace, out=sys.stdout) -> int:
    if not args.history_file.exists():
        print(f"error: file not found: {args.history_file}", file=sys.stderr)
        return 1

    grouped = _load_history(args.history_file)
    if not grouped:
        print("No records found.", file=out)
        return 0

    if args.pipeline:
        if args.pipeline not in grouped:
            print(f"error: pipeline '{args.pipeline}' not found in history", file=sys.stderr)
            return 1
        grouped = {args.pipeline: grouped[args.pipeline]}

    results: list[TrendResult] = []
    for snapshots in grouped.values():
        results.append(compute_trend(snapshots, min_points=args.min_points, threshold=args.threshold))

    for r in rank_by_trend(results):
        print(str(r), file=out)

    return 0


def main() -> None:
    parser = build_trend_parser()
    args = parser.parse_args()
    sys.exit(run_trend_command(args))


if __name__ == "__main__":
    main()
