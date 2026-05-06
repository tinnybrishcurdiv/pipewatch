"""CLI entry-point for the replay command."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipewatch.replay import replay_file


def build_replay_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    description = "Replay a history file through the metrics collector."
    if parent is not None:
        parser = parent.add_parser("replay", help=description)
    else:
        parser = argparse.ArgumentParser(prog="pipewatch-replay", description=description)

    parser.add_argument("file", type=Path, help="Path to NDJSON history file.")
    parser.add_argument("pipeline", help="Pipeline name to replay.")
    parser.add_argument(
        "--window", type=int, default=60, metavar="SECS",
        help="Collector sliding-window in seconds (default: 60).",
    )
    parser.add_argument(
        "--snapshot-every", type=int, default=10, metavar="N",
        help="Capture a snapshot every N records (default: 10).",
    )
    parser.add_argument(
        "--max-records", type=int, default=None, metavar="N",
        help="Stop after N records.",
    )
    parser.add_argument(
        "--json", dest="output_json", action="store_true",
        help="Emit snapshots as JSON to stdout.",
    )
    return parser


def run_replay_command(args: argparse.Namespace) -> int:
    if not args.file.exists():
        print(f"error: file not found: {args.file}", file=sys.stderr)
        return 1

    result = replay_file(
        path=args.file,
        pipeline=args.pipeline,
        window=args.window,
        snapshot_every=args.snapshot_every,
        max_records=args.max_records,
    )

    if args.output_json:
        print(json.dumps({"pipeline": result.pipeline, "records_fed": result.records_fed, "snapshots": result.snapshots}, indent=2))
    else:
        print(f"Pipeline : {result.pipeline}")
        print(f"Records  : {result.records_fed}")
        print(f"Snapshots: {len(result.snapshots)}")
        for i, snap in enumerate(result.snapshots, 1):
            sr = snap.get("success_rate")
            sr_str = f"{sr:.1%}" if sr is not None else "n/a"
            print(f"  [{i:>3}] success_rate={sr_str}  throughput={snap.get('throughput_rps', 0):.2f} rps")

    return 0


def main() -> None:  # pragma: no cover
    parser = build_replay_parser()
    args = parser.parse_args()
    sys.exit(run_replay_command(args))


if __name__ == "__main__":  # pragma: no cover
    main()
