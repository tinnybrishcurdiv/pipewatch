"""Tests for pipewatch.notifier."""
from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertFiring, AlertRule
from pipewatch.notifier import CompositeNotifier, SlackNotifier


def _make_firing() -> AlertFiring:
    rule = AlertRule(name="low-success", pipeline="pipe-a", metric="success_rate",
                     comparator="<", threshold=0.9)
    return AlertFiring(rule=rule, pipeline="pipe-a", metric="success_rate",
                       value=0.75, threshold=0.9)


class TestSlackNotifier:
    def test_posts_json_to_webhook(self):
        firing = _make_firing()
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/test")

        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_response) as mock_open:
            with patch("urllib.request.Request") as mock_req:
                notifier.notify(firing)
                mock_req.assert_called_once()
                _, kwargs = mock_req.call_args
                data = mock_req.call_args[0][1]
                payload = json.loads(data.decode())
                assert "text" in payload
                assert "PipeWatch Alert" in payload["text"]

    def test_includes_channel_when_set(self):
        firing = _make_firing()
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/test",
                                 channel="#alerts")

        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        captured = {}

        def fake_request(url, data=None, headers=None, method=None):
            captured["payload"] = json.loads(data.decode())
            return MagicMock()

        with patch("urllib.request.urlopen", return_value=mock_response):
            with patch("urllib.request.Request", side_effect=fake_request):
                notifier.notify(firing)

        assert captured["payload"].get("channel") == "#alerts"


class TestCompositeNotifier:
    def test_calls_all_notifiers(self):
        firing = _make_firing()
        n1, n2 = MagicMock(), MagicMock()
        composite = CompositeNotifier(notifiers=[n1, n2])
        composite.notify(firing)
        n1.notify.assert_called_once_with(firing)
        n2.notify.assert_called_once_with(firing)

    def test_silent_errors_swallows_exceptions(self):
        firing = _make_firing()
        bad = MagicMock()
        bad.notify.side_effect = RuntimeError("network down")
        composite = CompositeNotifier(notifiers=[bad], silent_errors=True)
        composite.notify(firing)  # should not raise

    def test_non_silent_errors_propagate(self):
        firing = _make_firing()
        bad = MagicMock()
        bad.notify.side_effect = RuntimeError("network down")
        composite = CompositeNotifier(notifiers=[bad], silent_errors=False)
        with pytest.raises(RuntimeError, match="network down"):
            composite.notify(firing)

    def test_empty_notifiers_is_noop(self):
        firing = _make_firing()
        composite = CompositeNotifier(notifiers=[])
        composite.notify(firing)  # should not raise
