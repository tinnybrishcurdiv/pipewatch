"""Render an aggregated summary table to the terminal."""

from __future__ import annotations

from pipewatch.aggregator import AggregatedSummary
from pipewatch.display import _colorize


_SEP = "-" * 44


def render_summary(summary: AggregatedSummary) -> str:
    """Return a multi-line string representing the aggregated summary."""
    lines = [
        _SEP,
        _colorize("  PIPELINE SUMMARY", "cyan"),
        _SEP,
    ]

    lines.append(f"  Pipelines tracked : {summary.total_pipelines}")
    lines.append(
        "  Status breakdown  : "
        + _colorize(f"{summary.healthy} healthy", "green")
        + "  "
        + _colorize(f"{summary.degraded} degraded", "yellow")
        + "  "
        + _colorize(f"{summary.failing} failing", "red")
    )
    lines.append(f"  Total processed   : {summary.total_processed:,}")
    lines.append(f"  Total errors      : {summary.total_errors:,}")

    if summary.overall_success_rate is not None:
        pct = summary.overall_success_rate * 100
        color = "green" if pct >= 95 else ("yellow" if pct >= 80 else "red")
        lines.append(
            f"  Overall success   : {_colorize(f'{pct:.1f}%', color)}"
        )
    else:
        lines.append("  Overall success   : N/A")

    if summary.avg_success_rate is not None:
        pct = summary.avg_success_rate * 100
        color = "green" if pct >= 95 else ("yellow" if pct >= 80 else "red")
        lines.append(
            f"  Avg success rate  : {_colorize(f'{pct:.1f}%', color)}"
        )
    else:
        lines.append("  Avg success rate  : N/A")

    if summary.slowest_pipeline:
        lines.append(f"  Slowest pipeline  : {summary.slowest_pipeline}")
    if summary.fastest_pipeline and summary.fastest_pipeline != summary.slowest_pipeline:
        lines.append(f"  Fastest pipeline  : {summary.fastest_pipeline}")

    lines.append(_SEP)
    return "\n".join(lines)
