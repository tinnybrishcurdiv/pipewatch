"""Build notifiers from a configuration dictionary or JSON."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from pipewatch.notifier import CompositeNotifier, EmailNotifier, SlackNotifier

_SUPPORTED = ("slack", "email")


def _build_slack(cfg: Dict[str, Any]) -> SlackNotifier:
    if "webhook_url" not in cfg:
        raise ValueError("slack notifier requires 'webhook_url'")
    return SlackNotifier(
        webhook_url=cfg["webhook_url"],
        channel=cfg.get("channel"),
    )


def _build_email(cfg: Dict[str, Any]) -> EmailNotifier:
    required = ("smtp_host", "smtp_port", "sender", "recipients")
    for key in required:
        if key not in cfg:
            raise ValueError(f"email notifier requires '{key}'")
    if not isinstance(cfg["recipients"], list) or not cfg["recipients"]:
        raise ValueError("'recipients' must be a non-empty list")
    return EmailNotifier(
        smtp_host=cfg["smtp_host"],
        smtp_port=int(cfg["smtp_port"]),
        sender=cfg["sender"],
        recipients=cfg["recipients"],
        password=cfg.get("password"),
        use_tls=cfg.get("use_tls", True),
    )


def notifiers_from_dict(config: List[Dict[str, Any]],
                        silent_errors: bool = False) -> CompositeNotifier:
    """Build a CompositeNotifier from a list of notifier config dicts.

    Each dict must have a 'type' key of 'slack' or 'email'.
    """
    built = []
    for entry in config:
        kind = entry.get("type", "").lower()
        if kind == "slack":
            built.append(_build_slack(entry))
        elif kind == "email":
            built.append(_build_email(entry))
        else:
            raise ValueError(
                f"Unknown notifier type '{kind}'. Supported: {_SUPPORTED}"
            )
    return CompositeNotifier(notifiers=built, silent_errors=silent_errors)


def notifiers_from_json(json_str: str,
                        silent_errors: bool = False) -> CompositeNotifier:
    """Build a CompositeNotifier from a JSON string."""
    data = json.loads(json_str)
    if not isinstance(data, list):
        raise ValueError("Notifier config JSON must be a list of notifier objects")
    return notifiers_from_dict(data, silent_errors=silent_errors)
