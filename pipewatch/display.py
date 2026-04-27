"""Terminal display rendering for pipeline metrics."""

from datetime import datetime
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics, status_label, success_rate


COLOR_RESET = "\033[0m"
COLOR_GREEN = "\033[92m"
COLOR_YELLOW = "\033[93m"
COLOR_RED = "\033[91m"
COLOR_CYAN = "\033[96m"
COLOR_BOLD = "\033[1m"
COLOR_DIM = "\033[2m"


def _colorize(text: str, color: str) -> str:
    return f"{color}{text}{COLOR_RESET}"


def _status_color(label: str) -> str:
    mapping = {
        "healthy": COLOR_GREEN,
        "degraded": COLOR_YELLOW,
        "critical": COLOR_RED,
        "no data": COLOR_DIM,
    }
    return mapping.get(label, COLOR_RESET)


def format_rate(rate: Optional[float]) -> str:
    """Format a success rate as a percentage string."""
    if rate is None:
        return _colorize("N/A", COLOR_DIM)
    pct = rate * 100
    color = COLOR_GREEN if pct >= 90 else (COLOR_YELLOW if pct >= 70 else COLOR_RED)
    return _colorize(f"{pct:.1f}%", color)


def render_pipeline_row(metrics: PipelineMetrics) -> str:
    """Render a single pipeline's metrics as a formatted table row."""
    rate = success_rate(metrics)
    label = status_label(metrics)
    color = _status_color(label)

    name_col = f"{metrics.pipeline_name:<24}"
    success_col = f"{metrics.success_count:>8}"
    failure_col = f"{metrics.failure_count:>8}"
    rate_col = f"{format_rate(rate):>12}"
    status_col = _colorize(f"{label:<10}", color)
    last_seen = metrics.last_updated.strftime("%H:%M:%S") if metrics.last_updated else "--:--:--"
    time_col = f"{last_seen:>10}"

    return f"  {name_col} {success_col} {failure_col} {rate_col}  {status_col} {time_col}"


def render_header() -> str:
    """Render the table header."""
    title = _colorize("PipeWatch — Pipeline Health Monitor", COLOR_BOLD + COLOR_CYAN)
    timestamp = _colorize(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), COLOR_DIM)
    header_row = (
        f"  {'PIPELINE':<24} {'SUCCESS':>8} {'FAILURE':>8} {'RATE':>12}  {'STATUS':<10} {'LAST SEEN':>10}"
    )
    separator = "-" * 80
    return f"\n{title}   {timestamp}\n{separator}\n{_colorize(header_row, COLOR_BOLD)}\n{separator}"


def render_dashboard(pipeline_metrics: List[PipelineMetrics]) -> str:
    """Render the full dashboard as a string."""
    lines = [render_header()]
    if not pipeline_metrics:
        lines.append(_colorize("  No pipelines being tracked.", COLOR_DIM))
    else:
        for m in sorted(pipeline_metrics, key=lambda x: x.pipeline_name):
            lines.append(render_pipeline_row(m))
    lines.append("-" * 80)
    lines.append(_colorize(f"  Tracking {len(pipeline_metrics)} pipeline(s). Press Ctrl+C to exit.", COLOR_DIM))
    return "\n".join(lines) + "\n"
