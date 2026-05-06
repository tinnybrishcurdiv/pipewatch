"""CLI entry point for capacity planning reports."""
from __future__ import annotations

import argparse
import sys

from pipewatch.capacity import compute_capacity, rank_by_headroom
from pipewatch.capacity_config import capacity_config_from_json, default_capacity_config
from pipewatch.snapshot import SnapshotManager


def build_capacity_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-capacity",
        description="Show capacity headroom for monitored pipelines.",
    )
    p.add_argument(
        "--config", metavar="FILE",
        help="JSON file with capacity configuration.",
    )
    p.add_argument(
        "--at-risk-threshold", type=float, default=None,
        metavar="PCT",
        help="Override at-risk threshold percentage (default: 80).",
    )
    p.add_argument(
        "--sort", choices=["headroom", "name"], default="headroom",
        help="Sort order for output (default: headroom).",
    )
    return p


def run_capacity_command(
    args: argparse.Namespace,
    manager: SnapshotManager,
    out=sys.stdout,
) -> None:
    if args.config:
        with open(args.config) as fh:
            cfg = capacity_config_from_json(fh.read())
    else:
        cfg = default_capacity_config()

    if args.at_risk_threshold is not None:
        cfg.at_risk_threshold = args.at_risk_threshold

    results = []
    for name, snap in manager.snapshots().items():
        metrics = snap.collector.latest()
        peak_cap = cfg.peak_capacity_for(name)
        result = compute_capacity(
            pipeline=name,
            metrics=metrics,
            peak_capacity=peak_cap,
            at_risk_threshold=cfg.at_risk_threshold,
        )
        results.append(result)

    if args.sort == "headroom":
        results = rank_by_headroom(results)
    else:
        results.sort(key=lambda r: r.pipeline)

    if not results:
        out.write("No pipelines registered.\n")
        return

    out.write(f"{'Pipeline':<30} {'Avg TPS':>10} {'Peak TPS':>10} {'Headroom':>10} {'Risk':>8}\n")
    out.write("-" * 72 + "\n")
    for r in results:
        avg = f"{r.current_tps:.2f}" if r.current_tps is not None else "n/a"
        peak = f"{r.peak_tps:.2f}" if r.peak_tps is not None else "n/a"
        head = f"{r.headroom_pct:.1f}%" if r.headroom_pct is not None else "n/a"
        risk = "YES" if r.at_risk else "-"
        out.write(f"{r.pipeline:<30} {avg:>10} {peak:>10} {head:>10} {risk:>8}\n")


def main() -> None:  # pragma: no cover
    parser = build_capacity_parser()
    args = parser.parse_args()
    manager = SnapshotManager()
    run_capacity_command(args, manager)


if __name__ == "__main__":  # pragma: no cover
    main()
