"""Collect run metrics and write them to observability.pipeline_runs."""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path

log = logging.getLogger(__name__)


def summarize_dbt_results(path: Path) -> dict[str, int]:
    """Count statuses in a dbt run_results.json ({'success': 12, 'warn': 3, ...})."""
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return dict(Counter(r["status"] for r in data.get("results", [])))


def parse_json_line(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(str(value).strip().splitlines()[-1])
    except (ValueError, IndexError):
        log.warning("Could not parse JSON from %r", value)
        return {}


UPSERT_SQL = """
INSERT INTO observability.pipeline_runs (
    run_id, dag_id, logical_date, status, started_at, finished_at, duration_seconds,
    records_extracted, records_loaded, records_rejected, rejection_rate_pct,
    dbt_models_ok, dbt_models_error, dbt_tests_pass, dbt_tests_warn, dbt_tests_fail,
    failed_tasks, error_message
) VALUES (
    %(run_id)s, %(dag_id)s, %(logical_date)s, %(status)s, %(started_at)s, %(finished_at)s,
    %(duration_seconds)s, %(records_extracted)s, %(records_loaded)s, %(records_rejected)s,
    %(rejection_rate_pct)s, %(dbt_models_ok)s, %(dbt_models_error)s, %(dbt_tests_pass)s,
    %(dbt_tests_warn)s, %(dbt_tests_fail)s, %(failed_tasks)s, %(error_message)s
)
ON CONFLICT (run_id) DO UPDATE SET
    status = EXCLUDED.status,
    finished_at = EXCLUDED.finished_at,
    duration_seconds = EXCLUDED.duration_seconds,
    records_extracted = EXCLUDED.records_extracted,
    records_loaded = EXCLUDED.records_loaded,
    records_rejected = EXCLUDED.records_rejected,
    rejection_rate_pct = EXCLUDED.rejection_rate_pct,
    dbt_models_ok = EXCLUDED.dbt_models_ok,
    dbt_models_error = EXCLUDED.dbt_models_error,
    dbt_tests_pass = EXCLUDED.dbt_tests_pass,
    dbt_tests_warn = EXCLUDED.dbt_tests_warn,
    dbt_tests_fail = EXCLUDED.dbt_tests_fail,
    failed_tasks = EXCLUDED.failed_tasks,
    error_message = EXCLUDED.error_message,
    recorded_at = now()
"""
