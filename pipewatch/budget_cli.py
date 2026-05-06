"""CLI entry-point for the error-budget sub-command."""
from __future__ import annotations

import argparse
import sys
from typing import Dict

from pipewatch.budget import BudgetResult, compute_budget, rank_by_budget
from pipewatch.budget_config import default_budget_config, budget_config_from_json
from pipewatch.metrics import PipelineMetrics
from pipewatch.snapshot import SnapshotManager


def build_budget_parser(sub: "argparse._SubParsersAction | None" = None) -> argparse.ArgumentParser:  # noqa: E501
    kwargs: dict = dict(
        description="Show error-budget consumption per pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = (
        sub.add_parser("budget", **kwargs) if sub else argparse.ArgumentParser(**kwargs)
    )
    parser.add_argument(
        "--slo", type=float, default=0.99,
        help="Default SLO target (0 < slo < 1)",
    )
    parser.add_argument(
        "--config-json", default=None,
        help="JSON string with per-pipeline SLO overrides",
    )
    parser.add_argument(
        "--worst", type=int, default=0,
        help="Show only the N worst pipelines (0 = all)",
    )
    return parser


def run_budget_command(
    args: argparse.Namespace,
    snapshot: SnapshotManager,
    out=sys.stdout,
) -> None:
    cfg = (
        budget_config_from_json(args.config_json)
        if args.config_json
        else default_budget_config()
    )
    # Allow CLI --slo to override the config default when explicitly provided
    if args.slo != 0.99 or cfg.default_slo == 0.99:
        cfg = cfg.__class__(
            default_slo=args.slo,
            per_pipeline=cfg.per_pipeline,
        )

    all_metrics: Dict[str, PipelineMetrics] = snapshot.all_metrics()
    if not all_metrics:
        out.write("No pipeline data available.\n")
        return

    results: list[BudgetResult] = [
        compute_budget(name, m, slo_target=cfg.slo_for(name))
        for name, m in all_metrics.items()
    ]
    ranked = rank_by_budget(results)
    if args.worst:
        ranked = ranked[: args.worst]

    for r in ranked:
        out.write(str(r) + "\n")


def main() -> None:  # pragma: no cover
    parser = build_budget_parser()
    args = parser.parse_args()
    sm = SnapshotManager()
    run_budget_command(args, sm)


if __name__ == "__main__":  # pragma: no cover
    main()
