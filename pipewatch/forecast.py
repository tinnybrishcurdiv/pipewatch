"""Simple linear extrapolation forecast for pipeline metrics."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.trend import TrendPoint, _to_point
from pipewatch.metrics import PipelineMetrics


@dataclass
class ForecastResult:
    pipeline: str
    metric: str
    horizon: int  # seconds into the future
    predicted_value: Optional[float]
    confidence: str  # "high" | "medium" | "low" | "insufficient_data"
    slope: Optional[float]

    def __str__(self) -> str:
        if self.predicted_value is None:
            return f"{self.pipeline} [{self.metric}]: insufficient data for forecast"
        direction = "up" if (self.slope or 0) > 0 else "down" if (self.slope or 0) < 0 else "flat"
        return (
            f"{self.pipeline} [{self.metric}]: "
            f"predicted {self.predicted_value:.2f} in {self.horizon}s "
            f"(trend {direction}, confidence {self.confidence})"
        )


def _linear_fit(points: List[TrendPoint]) -> Optional[tuple[float, float]]:
    """Return (slope, intercept) via least-squares, or None if not enough data."""
    n = len(points)
    if n < 2:
        return None
    xs = [p.timestamp for p in points]
    ys = [p.value for p in points]
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    denom = sum((x - x_mean) ** 2 for x in xs)
    if denom == 0:
        return None
    slope = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n)) / denom
    intercept = y_mean - slope * x_mean
    return slope, intercept


def _confidence(n: int, r_squared: float) -> str:
    if n < 3:
        return "low"
    if r_squared >= 0.85:
        return "high"
    if r_squared >= 0.5:
        return "medium"
    return "low"


def _r_squared(points: List[TrendPoint], slope: float, intercept: float) -> float:
    ys = [p.value for p in points]
    y_mean = sum(ys) / len(ys)
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    if ss_tot == 0:
        return 1.0
    ss_res = sum((p.value - (slope * p.timestamp + intercept)) ** 2 for p in points)
    return max(0.0, 1.0 - ss_res / ss_tot)


def forecast(
    history: Sequence[PipelineMetrics],
    metric: str,
    horizon: int = 300,
) -> ForecastResult:
    """Forecast *metric* for the pipeline *horizon* seconds from the last record."""
    if not history:
        name = "unknown"
        return ForecastResult(name, metric, horizon, None, "insufficient_data", None)

    name = history[0].pipeline_name
    points = [p for rec in history for p in [_to_point(rec, metric)] if p is not None]

    fit = _linear_fit(points)
    if fit is None:
        return ForecastResult(name, metric, horizon, None, "insufficient_data", None)

    slope, intercept = fit
    r2 = _r_squared(points, slope, intercept)
    conf = _confidence(len(points), r2)
    last_ts = max(p.timestamp for p in points)
    predicted = slope * (last_ts + horizon) + intercept
    return ForecastResult(name, metric, horizon, round(predicted, 4), conf, round(slope, 6))
