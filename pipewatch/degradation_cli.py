"""CLI entry-point for pipeline degradation detection."""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

from pipewatch.degradation import detect_degradation
from pipewatch.metrics import PipelineMetrics


def build_degradation_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-degradation",
        description="Detect degraded pipelines by comparing recent vs historical metrics.",
    )
    p.add_argument("history_file", help="Path to NDJSON history file.")
    p.add_argument(
        "--recent-minutes",
        type=int,
        default=15,
        metavar="N",
        help="Minutes of data considered 'recent' (default: 15).",
    )
    p.add_argument(
        "--baseline-minutes",
        type=int,
        default=60,
        metavar="N",
        help="Minutes of data used as baseline window (default: 60).",
    )
    p.add_argument(
        "--sr-drop",
        type=float,
        default=0.10,
        metavar="FRAC",
        help="Fractional success-rate drop threshold (default: 0.10).",
    )
    p.add_argument(
        "--tp-drop",
        type=float,
        default=0.20,
        metavar="FRAC",
        help="Fractional throughput drop threshold (default: 0.20).",
    )
    p.add_argument("--pipeline", metavar="NAME", help="Limit output to one pipeline.")
    return p


def _load_records(path: str) -> Dict[str, List[PipelineMetrics]]:
    """Return a dict mapping pipeline name → list of PipelineMetrics."""
    groups: Dict[str, List[PipelineMetrics]] = defaultdict(list)
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            m = PipelineMetrics(**{k: obj[k] for k in PipelineMetrics.__dataclass_fields__})
            groups[m.pipeline].append(m)
    return groups


def run_degradation_command(args: argparse.Namespace) -> int:
    import time

    now = time.time()
    recent_cutoff = now - args.recent_minutes * 60
    baseline_cutoff = now - args.baseline_minutes * 60

    try:
        all_records = _load_records(args.history_file)
    except FileNotFoundError:
        print(f"error: file not found: {args.history_file}", file=sys.stderr)
        return 1

    pipelines = [args.pipeline] if args.pipeline else sorted(all_records)
    found_degraded = False

    for name in pipelines:
        records = all_records.get(name, [])
        recent = [r for r in records if r.timestamp >= recent_cutoff]
        baseline = [r for r in records if baseline_cutoff <= r.timestamp < recent_cutoff]

        if not recent and not baseline:
            continue

        result = detect_degradation(
            name, recent, baseline,
            success_rate_drop=args.sr_drop,
            throughput_drop=args.tp_drop,
        )
        print(result)
        if result.degraded:
            found_degraded = True

    return 1 if found_degraded else 0


def main() -> None:  # pragma: no cover
    sys.exit(run_degradation_command(build_degradation_parser().parse_args()))
