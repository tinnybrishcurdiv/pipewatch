"""CLI commands for baseline capture and comparison."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.baseline import BaselineDiff, compare_to_baseline, load_baseline, save_baseline
from pipewatch.exporter import export_json
from pipewatch.snapshot import SnapshotManager

_DEFAULT_BASELINE = ".pipewatch_baseline.json"


def build_baseline_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch-baseline",
        description="Capture or compare pipeline metric baselines.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="Save current metrics as baseline.")
    cap.add_argument("--output", default=_DEFAULT_BASELINE, help="Path to baseline file.")

    cmp = sub.add_parser("compare", help="Compare current metrics to baseline.")
    cmp.add_argument("--baseline", default=_DEFAULT_BASELINE, help="Path to baseline file.")
    cmp.add_argument(
        "--format", choices=["text", "json"], default="text", dest="fmt"
    )
    cmp.add_argument(
        "--warn-below",
        type=float,
        default=None,
        metavar="DELTA",
        help="Exit with code 1 if any success_rate drops more than DELTA percent.",
    )
    return parser


def run_baseline_command(args: argparse.Namespace, manager: SnapshotManager) -> int:
    metrics_map = {name: snap.collector.latest() for name, snap in manager.snapshots.items()}
    # Drop pipelines with no data
    metrics_map = {k: v for k, v in metrics_map.items() if v is not None}

    if args.command == "capture":
        save_baseline(metrics_map, args.output)  # type: ignore[arg-type]
        print(f"Baseline saved to {args.output} ({len(metrics_map)} pipelines).")
        return 0

    # compare
    try:
        baseline = load_baseline(args.baseline)
    except FileNotFoundError:
        print(f"Baseline file not found: {args.baseline}", file=sys.stderr)
        return 2

    diffs = compare_to_baseline(metrics_map, baseline)  # type: ignore[arg-type]

    if args.fmt == "json":
        payload = [
            {
                "pipeline": d.pipeline,
                "success_rate_delta": d.success_rate_delta,
                "throughput_delta": d.throughput_delta,
            }
            for d in diffs.values()
        ]
        print(json.dumps(payload, indent=2))
    else:
        for d in diffs.values():
            print(d)

    if args.warn_below is not None:
        for d in diffs.values():
            if d.success_rate_delta is not None and d.success_rate_delta < -abs(args.warn_below):
                return 1
    return 0


def main() -> None:  # pragma: no cover
    parser = build_baseline_parser()
    args = parser.parse_args()
    manager = SnapshotManager()
    sys.exit(run_baseline_command(args, manager))
