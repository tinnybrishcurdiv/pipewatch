"""CLI entry-point for the retention / pruning sub-command."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipewatch.retention import RetentionPolicy, prune_directory, prune_file


def build_retention_parser(parent: argparse._SubParsersAction | None = None) -> argparse.ArgumentParser:  # type: ignore[type-arg]
    kwargs = dict(
        prog="pipewatch retention",
        description="Prune stale history records from *.jsonl files.",
    )
    if parent is not None:
        parser = parent.add_parser("retention", **kwargs)
    else:
        parser = argparse.ArgumentParser(**kwargs)

    parser.add_argument(
        "path",
        type=Path,
        help="Path to a single .jsonl file or a directory of .jsonl files.",
    )
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=30,
        metavar="N",
        help="Remove records older than N days (default: 30).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        metavar="N",
        help="Keep at most the N most-recent records per file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be removed without modifying files.",
    )
    return parser


def run_retention_command(args: argparse.Namespace) -> None:
    policy = RetentionPolicy(
        max_age_days=args.max_age_days,
        max_records_per_pipeline=args.max_records,
    )

    target: Path = args.path

    if args.dry_run:
        print("[dry-run] No files will be modified.")

    if target.is_dir():
        results = prune_directory(target, policy) if not args.dry_run else {}
        if args.dry_run:
            files = list(target.glob("*.jsonl"))
            print(f"Would inspect {len(files)} file(s) in {target}")
        else:
            total = sum(results.values())
            for fname, removed in results.items():
                if removed:
                    print(f"  {fname}: removed {removed} record(s)")
            print(f"Done. {total} record(s) pruned across {len(results)} file(s).")
    elif target.is_file():
        if args.dry_run:
            print(f"Would prune {target}")
        else:
            removed = prune_file(target, policy)
            print(f"{target.name}: removed {removed} record(s).")
    else:
        print(f"Error: {target} does not exist.", file=sys.stderr)
        sys.exit(1)


def main() -> None:  # pragma: no cover
    parser = build_retention_parser()
    run_retention_command(parser.parse_args())


if __name__ == "__main__":  # pragma: no cover
    main()
