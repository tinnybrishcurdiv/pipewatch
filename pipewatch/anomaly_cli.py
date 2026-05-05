"""CLI entry-point for anomaly detection against recorded history."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.anomaly import AnomalyResult, detect_anomalies
from pipewatch.metrics import PipelineMetrics


def build_anomaly_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipewatch-anomaly",
        description="Detect anomalous pipelines from history files.",
    )
    p.add_argument("history_file", help="NDJSON history file produced by pipewatch")
    p.add_argument(
        "--pipeline",
        default=None,
        help="Filter to a single pipeline name (default: all)",
    )
    p.add_argument(
        "--warning-z",
        type=float,
        default=2.0,
        dest="warning_z",
        help="Z-score threshold for warning (default: 2.0)",
    )
    p.add_argument(
        "--critical-z",
        type=float,
        default=3.0,
        dest="critical_z",
        help="Z-score threshold for critical (default: 3.0)",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    return p


def _load_records(path: Path) -> List[PipelineMetrics]:
    records: List[PipelineMetrics] = []
    with path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            records.append(PipelineMetrics(**{k: obj[k] for k in PipelineMetrics.__dataclass_fields__}))
    return records


def run_anomaly_command(args: argparse.Namespace) -> int:
    path = Path(args.history_file)
    if not path.exists():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 1

    records = _load_records(path)
    if args.pipeline:
        records = [r for r in records if r.pipeline == args.pipeline]

    pipelines = {r.pipeline for r in records}
    all_anomalies: List[AnomalyResult] = []
    for name in sorted(pipelines):
        pipeline_records = [r for r in records if r.pipeline == name]
        if len(pipeline_records) < 2:
            continue
        *history, current = pipeline_records
        all_anomalies.extend(
            detect_anomalies(current, history, args.warning_z, args.critical_z)
        )

    if not all_anomalies:
        print("No anomalies detected.")
        return 0

    if args.json:
        print(json.dumps([vars(a) for a in all_anomalies], indent=2))
    else:
        for a in all_anomalies:
            print(a)
    return 0


def main() -> None:
    sys.exit(run_anomaly_command(build_anomaly_parser().parse_args()))


if __name__ == "__main__":
    main()
