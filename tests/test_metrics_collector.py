"""Tests for PipelineMetrics and MetricsCollector."""

from datetime import datetime, timedelta

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.collector import MetricsCollector


def make_metric(name="pipe", processed=100, failed=0, healthy=True, latency=5.0, tps=50.0, error=None, age_seconds=0):
    m = PipelineMetrics(
        name=name,
        records_processed=processed,
        records_failed=failed,
        throughput_per_sec=tps,
        latency_ms=latency,
        is_healthy=healthy,
        error_message=error,
    )
    if age_seconds:
        m.timestamp = datetime.utcnow() - timedelta(seconds=age_seconds)
    return m


class TestPipelineMetrics:
    def test_success_rate_all_good(self):
        m = make_metric(processed=200, failed=0)
        assert m.success_rate == 100.0

    def test_success_rate_mixed(self):
        m = make_metric(processed=80, failed=20)
        assert m.success_rate == 80.0

    def test_success_rate_no_records(self):
        m = make_metric(processed=0, failed=0)
        assert m.success_rate == 100.0

    def test_status_label_healthy(self):
        m = make_metric(healthy=True)
        assert m.status_label == "OK"

    def test_status_label_degraded(self):
        m = make_metric(healthy=False)
        assert m.status_label == "DEGRADED"

    def test_to_dict_keys(self):
        m = make_metric()
        d = m.to_dict()
        expected_keys = {
            "name", "timestamp", "records_processed", "records_failed",
            "throughput_per_sec", "latency_ms", "success_rate",
            "is_healthy", "status_label", "error_message",
        }
        assert expected_keys == set(d.keys())


class TestMetricsCollector:
    def test_latest_returns_none_when_empty(self):
        c = MetricsCollector("pipe")
        assert c.latest() is None

    def test_latest_returns_most_recent(self):
        c = MetricsCollector("pipe")
        c.record(make_metric(processed=10))
        c.record(make_metric(processed=20))
        assert c.latest().records_processed == 20

    def test_stale_samples_evicted(self):
        c = MetricsCollector("pipe", window_seconds=30)
        c.record(make_metric(age_seconds=60))
        c.record(make_metric(processed=50))
        samples = c.window_samples()
        assert len(samples) == 1
        assert samples[0].records_processed == 50

    def test_aggregate_totals_processed(self):
        c = MetricsCollector("pipe")
        c.record(make_metric(processed=100))
        c.record(make_metric(processed=200))
        agg = c.aggregate()
        assert agg.records_processed == 300

    def test_aggregate_unhealthy_if_any_degraded(self):
        c = MetricsCollector("pipe")
        c.record(make_metric(healthy=True))
        c.record(make_metric(healthy=False, error="timeout"))
        agg = c.aggregate()
        assert not agg.is_healthy
        assert agg.error_message == "timeout"

    def test_aggregate_returns_none_when_empty(self):
        c = MetricsCollector("pipe")
        assert c.aggregate() is None
