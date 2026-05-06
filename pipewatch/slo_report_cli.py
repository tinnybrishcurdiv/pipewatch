"""CLI entry‑point for the SLO compliance report."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Dict

from pipewatch.snapshot import SnapshotManager
from pipewatch.slo_report import compute_slo_report, rank_by_gap


def build_slo_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-slo",
        description="Show SLO compliance for all tracked pipelines.",
    )
    p.add_argument(
        "--target",
        metavar="PIPELINE=VALUE",
        action="append",
        default=[],
        help="Override SLO target for a pipeline, e.g. orders=0.995. Use *=VALUE for global default.",
    )
    p.add_argument(
        "--sort",
        choices=["gap", "name"],
        default="gap",
        help="Sort order: worst gap first (default) or alphabetical.",
    )
    p.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit JSON instead of plain text.",
    )
    p.add_argument(
        "--fail-on-breach",
        action="store_true",
        help="Exit with code 1 if any pipeline is in breach.",
    )
    return p


def _parse_targets(raw: list) -> Dict[str, float]:
    targets: Dict[str, float] = {}
    for item in raw:
        if "=" not in item:
            raise SystemExit(f"Invalid --target format (expected PIPELINE=VALUE): {item!r}")
        name, val = item.split("=", 1)
        try:
            targets[name.strip()] = float(val.strip())
        except ValueError:
            raise SystemExit(f"Invalid SLO value {val!r} for pipeline {name!r}")
    return targets


def run_slo_command(args: argparse.Namespace, manager: SnapshotManager) -> int:
    targets = _parse_targets(args.target)
    metrics_map = {name: snap.collector.latest() for name, snap in manager.snapshots.items()}
    results = compute_slo_report(metrics_map, targets)

    if args.sort == "gap":
        results = rank_by_gap(results)

    if args.as_json:
        payload = [
            {
                "pipeline": r.pipeline,
                "target": r.target,
                "actual": r.actual,
                "compliant": r.compliant,
                "gap_pp": r.gap(),
            }
            for r in results
        ]
        print(json.dumps(payload, indent=2))
    else:
        for r in results:
            print(r)

    breached = any(not r.compliant for r in results)
    if args.fail_on_breach and breached:
        return 1
    return 0


def main() -> None:  # pragma: no cover
    parser = build_slo_parser()
    args = parser.parse_args()
    manager = SnapshotManager()
    sys.exit(run_slo_command(args, manager))
