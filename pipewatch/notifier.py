"""Alert notification channels for pipewatch."""
from __future__ import annotations

import json
import smtplib
import urllib.request
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import List, Optional

from pipewatch.alerts import AlertFiring


@dataclass
class SlackNotifier:
    """Send alert notifications to a Slack webhook URL."""

    webhook_url: str
    channel: Optional[str] = None

    def notify(self, firing: AlertFiring) -> None:
        payload: dict = {
            "text": f":rotating_light: *PipeWatch Alert* — {firing}",
        }
        if self.channel:
            payload["channel"] = self.channel
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass


@dataclass
class EmailNotifier:
    """Send alert notifications via SMTP."""

    smtp_host: str
    smtp_port: int
    sender: str
    recipients: List[str]
    password: Optional[str] = None
    use_tls: bool = True

    def notify(self, firing: AlertFiring) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"[PipeWatch] Alert: {firing.rule.name} on {firing.pipeline}"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.set_content(str(firing))

        if self.use_tls:
            server = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
        else:
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)

        with server:
            if self.password:
                server.login(self.sender, self.password)
            server.send_message(msg)


@dataclass
class CompositeNotifier:
    """Fan-out notifications to multiple notifiers."""

    notifiers: List = field(default_factory=list)
    silent_errors: bool = False

    def notify(self, firing: AlertFiring) -> None:
        for notifier in self.notifiers:
            try:
                notifier.notify(firing)
            except Exception:
                if not self.silent_errors:
                    raise
