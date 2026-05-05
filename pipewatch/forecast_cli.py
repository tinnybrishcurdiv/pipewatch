"""CLI entry point for the forecast command."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipewatch.metrics import PipelineMetrics
from pipewatch.forecast import forecast, ForecastResult

_VALID_METRICS = ("success_rate", "throughput")


def build_forecast_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    kwargs = dict(description="Forecast a pipeline metric using linear extrapolation.")
    parser = parent.add_parser("forecast", **kwargs) if parent else argparse.ArgumentParser(**kwargs)
    parser.add_argument("file", help="NDJSON history file produced by pipewatch history")
    parser.add_argument("--pipeline", default="*", help="Pipeline name or '*' for all (default: *)")
    parser.add_argument(
        "--metric",
        choices=_VALID_METRICS,
        default="success_rate",
        help="Metric to forecast (default: success_rate)",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=300,
        metavar="SECONDS",
        help="Seconds into the future to predict (default: 300)",
    )
    parser.add_argument("--json", dest="as_json", action="store_true", help="Output as JSON")
    return parser


def _load_records(path: str) -> List[PipelineMetrics]:
    records: List[PipelineMetrics] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            records.append(PipelineMetrics(**data))
    return records


def run_forecast_command(args: argparse.Namespace) -> int:
    try:
        all_records = _load_records(args.file)
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    pipelines = {r.pipeline_name for r in all_records}
    selected = pipelines if args.pipeline == "*" else {args.pipeline}
    missing = selected - pipelines
    if missing:
        print(f"error: unknown pipeline(s): {', '.join(sorted(missing))}", file=sys.stderr)
        return 1

    results: List[ForecastResult] = []
    for name in sorted(selected):
        history = [r for r in all_records if r.pipeline_name == name]
        results.append(forecast(history, args.metric, args.horizon))

    if args.as_json:
        print(json.dumps([r.__dict__ for r in results], indent=2))
    else:
        for r in results:
            print(r)
    return 0


def main() -> None:  # pragma: no cover
    parser = build_forecast_parser()
    sys.exit(run_forecast_command(parser.parse_args()))


if __name__ == "__main__":  # pragma: no cover
    main()
