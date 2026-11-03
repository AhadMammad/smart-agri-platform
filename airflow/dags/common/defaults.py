"""Default task arguments every DAG shares: who owns it, and who hears when it fails.

A failed task is the only signal an error-severity data quality check gives —
the run stops, and nothing downstream runs. Without an email that signal waits
for someone to open the Airflow UI.

Recipients come from `ALERT_EMAIL_TO` (comma-separated); the SMTP server from
the `AIRFLOW__SMTP__*` variables Compose sets — Mailpit unless overridden. With
no recipients configured, nothing is sent and nothing breaks.
"""

from __future__ import annotations

import os
from typing import Any


def alert_recipients() -> list[str]:
    """Addresses from `ALERT_EMAIL_TO`, ignoring blanks."""
    raw = os.environ.get("ALERT_EMAIL_TO", "")
    return [address.strip() for address in raw.split(",") if address.strip()]


def with_alerts(default_args: dict[str, Any]) -> dict[str, Any]:
    """A DAG's own `default_args`, plus failure email to the alert recipients.

    Email on the final failure only, never on a retry: a task with two retries
    would otherwise send three messages about one problem.
    """
    recipients = alert_recipients()
    return {
        "email": recipients,
        "email_on_failure": bool(recipients),
        "email_on_retry": False,
        **default_args,
    }
