"""CLI sub-command: inspect current rate-limiter token levels."""
from __future__ import annotations

import argparse
import json
import sys
from typing import List

from pipewatch.rate_limiter_config import default_rate_limiter, rate_limiter_from_json
from pipewatch.rate_limiter import RateLimiter


def build_rate_limiter_parser(subparsers=None) -> argparse.ArgumentParser:
    description = "Inspect or test the per-pipeline rate limiter."
    if subparsers is not None:
        parser = subparsers.add_parser("rate-limiter", description=description)
    else:
        parser = argparse.ArgumentParser(prog="pipewatch-rate-limiter",
                                         description=description)
    parser.add_argument(
        "pipelines",
        nargs="+",
        metavar="PIPELINE",
        help="Pipeline names to query.",
    )
    parser.add_argument(
        "--config",
        metavar="JSON",
        default=None,
        help="Inline JSON rate-limiter config (optional).",
    )
    parser.add_argument(
        "--consume",
        action="store_true",
        help="Consume one token per pipeline and report allow/deny.",
    )
    return parser


def run_rate_limiter_command(
    pipelines: List[str],
    config_json: str | None = None,
    consume: bool = False,
) -> str:
    """Return a formatted string showing token levels (or allow/deny)."""
    if config_json:
        limiter: RateLimiter = rate_limiter_from_json(config_json)
    else:
        limiter = default_rate_limiter()

    rows = []
    for name in pipelines:
        if consume:
            allowed = limiter.allow(name)
            status = "ALLOW" if allowed else "DENY"
            rows.append({"pipeline": name, "status": status})
        else:
            tokens = limiter.available_tokens(name)
            rows.append({"pipeline": name, "available_tokens": round(tokens, 3)})

    return json.dumps(rows, indent=2)


def main(argv: List[str] | None = None) -> None:
    parser = build_rate_limiter_parser()
    args = parser.parse_args(argv)
    output = run_rate_limiter_command(
        pipelines=args.pipelines,
        config_json=args.config,
        consume=args.consume,
    )
    print(output)


if __name__ == "__main__":  # pragma: no cover
    main()
