"""Failure / warning alerts: Slack incoming webhook and (optionally) email.

Both are optional and configured with environment variables:
  SLACK_WEBHOOK_URL   https://hooks.slack.com/services/...  (empty = disabled)
  ALERT_EMAIL         comma-separated addresses; also needs AIRFLOW__SMTP__* settings
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger(__name__)


def send_slack(text: str, *, emoji: str = ":rotating_light:") -> bool:
    url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not url:
        log.info("SLACK_WEBHOOK_URL not set; alert only logged:\n%s", text)
        return False
    payload = json.dumps({"text": f"{emoji} {text}"}).encode()
    request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except Exception:  # noqa: BLE001 - an alert failing must never break the pipeline
        log.exception("Could not send Slack alert")
        return False


def alert_emails() -> list[str]:
    return [e.strip() for e in os.getenv("ALERT_EMAIL", "").split(",") if e.strip()]


def task_failure_alert(context: dict) -> None:
    """on_failure_callback for tasks."""
    ti = context["task_instance"]
    exception = str(context.get("exception") or "")[:500]
    text = (
        f"*DataFlow task failed*\n"
        f"• DAG: `{ti.dag_id}`\n"
        f"• Task: `{ti.task_id}` (try {ti.try_number - 1} of {ti.max_tries + 1})\n"
        f"• Run: `{context['run_id']}`\n"
        f"• Error: ```{exception}```\n"
        f"• Logs: {ti.log_url}"
    )
    send_slack(text)
