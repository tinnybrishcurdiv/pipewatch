"""CLI entry-point for cross-pipeline correlation analysis."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

from pipewatch.metrics import PipelineMetrics
from pipewatch.correlation import compute_correlations, CorrelationResult


def build_correlation_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-correlation",
        description="Show cross-pipeline success-rate correlations.",
    )
    p.add_argument(
        "history_file",
        help="NDJSON history file produced by pipewatch history.",
    )
    p.add_argument(
        "--min-points",
        type=int,
        default=3,
        metavar="N",
        help="Minimum data points required per pipeline (default: 3).",
    )
    p.add_argument(
        "--min-strength",
        choices=["strong", "moderate", "weak", "none"],
        default="weak",
        help="Only show pairs at or above this strength (default: weak).",
    )
    p.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Output results as JSON.",
    )
    return p


_STRENGTH_ORDER = {"none": 0, "weak": 1, "moderate": 2, "strong": 3}


def _load_history(path: str) -> Dict[str, List[PipelineMetrics]]:
    history: Dict[str, List[PipelineMetrics]] = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        name = obj["pipeline"]
        m = PipelineMetrics(
            pipeline=name,
            total=obj.get("total", 0),
            success=obj.get("success", 0),
            failure=obj.get("failure", 0),
            throughput_per_sec=obj.get("throughput_per_sec"),
            avg_latency_ms=obj.get("avg_latency_ms"),
        )
        history.setdefault(name, []).append(m)
    return history


def run_correlation_command(args: argparse.Namespace) -> None:
    history = _load_history(args.history_file)
    results = compute_correlations(history, min_points=args.min_points)
    min_order = _STRENGTH_ORDER[args.min_strength]
    filtered = [r for r in results if _STRENGTH_ORDER[r.strength] >= min_order]

    if args.as_json:
        payload = [
            {
                "pipeline_a": r.pipeline_a,
                "pipeline_b": r.pipeline_b,
                "coefficient": r.coefficient,
                "strength": r.strength,
            }
            for r in filtered
        ]
        print(json.dumps(payload, indent=2))
    else:
        if not filtered:
            print("No correlated pipeline pairs found.")
            return
        for r in filtered:
            print(r)


def main() -> None:  # pragma: no cover
    parser = build_correlation_parser()
    run_correlation_command(parser.parse_args())


if __name__ == "__main__":  # pragma: no cover
    main()
