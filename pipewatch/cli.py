"""CLI entry point for pipewatch — real-time pipeline health monitor."""

import time
import argparse
import sys
from typing import Optional

from pipewatch.collector import MetricsCollector
from pipewatch.display import render_header, render_pipeline_row


DEFAULT_REFRESH_INTERVAL = 2.0
DEFAULT_WINDOW_SECONDS = 60


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipewatch",
        description="Monitor the health and throughput of long-running data pipelines.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_REFRESH_INTERVAL,
        metavar="SECONDS",
        help=f"Refresh interval in seconds (default: {DEFAULT_REFRESH_INTERVAL})",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW_SECONDS,
        metavar="SECONDS",
        help=f"Rolling window for metrics in seconds (default: {DEFAULT_WINDOW_SECONDS})",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render a single snapshot and exit (useful for scripting/testing)",
    )
    return parser


def render_snapshot(collector: MetricsCollector) -> None:
    """Print a full dashboard snapshot to stdout."""
    pipelines = collector.pipeline_names()
    print(render_header())
    if not pipelines:
        print("  (no pipeline data recorded yet)")
        return
    for name in sorted(pipelines):
        metrics = collector.latest(name)
        if metrics is not None:
            print(render_pipeline_row(name, metrics))


def run(collector: MetricsCollector, args: Optional[argparse.Namespace] = None) -> None:
    """Main loop — renders dashboard snapshots at the configured interval."""
    if args is None:
        args = build_parser().parse_args([])

    if args.once:
        render_snapshot(collector)
        return

    try:
        while True:
            # Clear terminal and re-render
            print("\033[2J\033[H", end="")
            render_snapshot(collector)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\npipewatch stopped.")
        sys.exit(0)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    collector = MetricsCollector(window_seconds=args.window)
    run(collector, args)


if __name__ == "__main__":
    main()
