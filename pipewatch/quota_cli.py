"""CLI sub-command: quota — show per-pipeline throughput quota utilisation."""
from __future__ import annotations

import argparse
import sys
from typing import List

from pipewatch.quota import QuotaConfig, QuotaResult, compute_quota, rank_by_utilisation
from pipewatch.quota_config import default_quota_config, quota_config_from_json
from pipewatch.snapshot import SnapshotManager


def build_quota_parser(sub: "argparse._SubParsersAction") -> argparse.ArgumentParser:  # type: ignore[type-arg]
    p = sub.add_parser("quota", help="Show throughput quota utilisation per pipeline")
    p.add_argument("--config", metavar="JSON", default=None,
                   help="Inline JSON quota configuration")
    p.add_argument("--exceeded-only", action="store_true",
                   help="Show only pipelines that have exceeded their quota")
    p.add_argument("--top", type=int, default=0,
                   help="Limit output to top N pipelines by utilisation (0 = all)")
    return p


def run_quota_command(
    manager: SnapshotManager,
    config: QuotaConfig,
    exceeded_only: bool = False,
    top: int = 0,
) -> List[QuotaResult]:
    results: List[QuotaResult] = []
    for name in manager.pipeline_names():
        snap = manager.snapshot(name)
        if snap is None:
            continue
        metrics = snap.latest()
        if metrics is None:
            continue
        results.append(compute_quota(metrics, config))

    ranked = rank_by_utilisation(results)

    if exceeded_only:
        ranked = [r for r in ranked if r.exceeded]
    if top > 0:
        ranked = ranked[:top]
    return ranked


def main(argv: list[str] | None = None) -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(prog="pipewatch-quota")
    subs = parser.add_subparsers(dest="cmd")
    build_quota_parser(subs)
    args = parser.parse_args(argv)

    cfg = quota_config_from_json(args.config) if args.config else default_quota_config()
    manager = SnapshotManager()

    results = run_quota_command(
        manager,
        cfg,
        exceeded_only=getattr(args, "exceeded_only", False),
        top=getattr(args, "top", 0),
    )

    if not results:
        print("No quota data available.")
        sys.exit(0)

    for r in results:
        print(r)
