"""Core metrics data structures for pipeline health tracking."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetrics:
    """Snapshot of pipeline health and throughput metrics."""

    name: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    records_processed: int = 0
    records_failed: int = 0
    throughput_per_sec: float = 0.0
    latency_ms: float = 0.0
    is_healthy: bool = True
    error_message: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Return the success rate as a percentage (0-100)."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 100.0
        return (self.records_processed / total) * 100.0

    @property
    def status_label(self) -> str:
        """Human-readable health status."""
        return "OK" if self.is_healthy else "DEGRADED"

    def to_dict(self) -> dict:
        """Serialize metrics to a plain dictionary."""
        return {
            "name": self.name,
            "timestamp": self.timestamp.isoformat(),
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "throughput_per_sec": round(self.throughput_per_sec, 2),
            "latency_ms": round(self.latency_ms, 2),
            "success_rate": round(self.success_rate, 2),
            "is_healthy": self.is_healthy,
            "status_label": self.status_label,
            "error_message": self.error_message,
        }
