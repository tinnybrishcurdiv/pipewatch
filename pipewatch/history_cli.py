"""CLI sub-commands for querying and exporting pipeline history."""

from __future__ import annotations

import argparse
import json
import sys

from pipewatch.history import HistoryReader, DEFAULT_HISTORY_PATH


def build_history_parser(subparsers: argparse._SubParsersAction) -> None:
    """Register 'history' sub-commands onto an existing subparsers group."""
    hist = subparsers.add_parser("history", help="Query pipeline history log")
    hist_sub = hist.add_subparsers(dest="history_cmd", required=True)

    # history show
    show = hist_sub.add_parser("show", help="Print history entries as JSON")
    show.add_argument("--pipeline", "-p", default=None, help="Filter by pipeline name")
    show.add_argument(
        "--file", "-f", default=DEFAULT_HISTORY_PATH, help="History log path"
    )
    show.add_argument("--last", "-n", type=int, default=None, help="Show last N entries")

    # history export-csv
    export = hist_sub.add_parser("export-csv", help="Export history to CSV")
    export.add_argument("dest", help="Destination CSV file path")
    export.add_argument(
        "--file", "-f", default=DEFAULT_HISTORY_PATH, help="History log path"
    )


def run_history_command(args: argparse.Namespace) -> int:
    """Dispatch a parsed history sub-command. Returns exit code."""
    reader = HistoryReader(args.file)

    if args.history_cmd == "show":
        entries = (
            reader.read_pipeline(args.pipeline)
            if args.pipeline
            else reader.read_all()
        )
        if args.last is not None:
            entries = entries[-args.last :]
        if not entries:
            print("No history entries found.", file=sys.stderr)
            return 1
        print(json.dumps(entries, indent=2))
        return 0

    if args.history_cmd == "export-csv":
        count = reader.export_csv(args.dest)
        if count == 0:
            print("No entries to export.", file=sys.stderr)
            return 1
        print(f"Exported {count} entries to {args.dest}")
        return 0

    print(f"Unknown history command: {args.history_cmd}", file=sys.stderr)
    return 2


def main(argv: list[str] | None = None) -> int:
    """Standalone entry point for history CLI."""
    parser = argparse.ArgumentParser(prog="pipewatch-history")
    subparsers = parser.add_subparsers(dest="cmd", required=True)
    build_history_parser(subparsers)
    # history sub-commands are nested; unwrap one level for standalone use
    args = parser.parse_args(argv)
    return run_history_command(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
