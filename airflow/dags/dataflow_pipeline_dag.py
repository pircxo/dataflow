"""
### DataFlow – daily e-commerce pipeline

```
simulate_source_data ─► check_source_availability ─► run_ingestion ─► dbt_run ─► dbt_test ─► log_run_metrics
                                                                                              ▲ (runs even on failure)
                                                                     watcher (fails the run if anything failed)
```

| Task | What it does |
|------|--------------|
| `simulate_source_data` | Demo only: makes the fake shop produce the day's data (no-op if it already exists). Disable with param `simulate_sources=false`. |
| `check_source_availability` | Source DB, payments API, file drops and warehouse are reachable. Fails fast before any work. |
| `run_ingestion` | Incremental, idempotent load into `raw`; bad rows into `quarantine`. Pushes a JSON summary to XCom. |
| `dbt_run` | Source freshness check, customer snapshot (SCD2), then all models. |
| `dbt_test` | Data tests. `error` tests fail the task; `warn` tests are only reported. |
| `log_run_metrics` | Writes one row to `observability.pipeline_runs` (always runs). |
| `watcher` | Makes the DAG run FAILED if any task failed, even though the metrics task succeeded. |

**Params** (Trigger DAG w/ config): `full_refresh`, `backfill_since` (YYYY-MM-DD), `simulate_sources`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from dataflow_utils.alerts import alert_emails, send_slack, task_failure_alert
from dataflow_utils.metrics import UPSERT_SQL, parse_json_line, summarize_dbt_results

DAG_ID = "dataflow_pipeline"
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", f"{PROJECT_ROOT}/dbt_dataflow")
PIPELINE_PYTHON = os.getenv("PIPELINE_PYTHON", "/opt/pipeline_venv/bin/python")
DBT_BIN = os.getenv("DBT_BIN", "/opt/pipeline_venv/bin/dbt")
WAREHOUSE_CONN_ID = "dataflow_warehouse"
REJECTION_ALERT_PCT = float(os.getenv("REJECTION_ALERT_PCT", "15"))

# dbt artifacts of each run are copied here so the metrics task can read them.
DBT_ARTIFACTS = DBT_PROJECT_DIR + "/target/runs/{{ ts_nodash }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,  # 2m, 4m, 8m ... capped below
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": task_failure_alert,
    "email": alert_emails(),
    "email_on_failure": bool(alert_emails()),
    "email_on_retry": False,
}


def _dbt(command: str, artifact_name: str) -> str:
    """Run a dbt command and keep its run_results.json, preserving the exit code."""
    return f"""
set -uo pipefail
cd "{DBT_PROJECT_DIR}"
{command}
rc=$?
mkdir -p "{DBT_ARTIFACTS}"
cp target/run_results.json "{DBT_ARTIFACTS}/{artifact_name}.json" 2>/dev/null || true
exit $rc
"""


def log_run_metrics(**context) -> None:
    """Write one row per DAG run into observability.pipeline_runs."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    dag_run, ti = context["dag_run"], context["ti"]
    failed_states = {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
    failed = sorted(
        t.task_id
        for t in dag_run.get_task_instances()
        if t.state in failed_states and t.task_id not in (ti.task_id, "watcher")
    )

    ingestion = parse_json_line(ti.xcom_pull(task_ids="run_ingestion"))
    artifacts = Path(DBT_PROJECT_DIR) / "target" / "runs" / context["ts_nodash"]
    dbt_run = summarize_dbt_results(artifacts / "run.json")
    dbt_test = summarize_dbt_results(artifacts / "test.json")

    extracted = ingestion.get("records_extracted") or 0
    rejected = ingestion.get("records_rejected") or 0
    rejection_pct = round(100.0 * rejected / extracted, 2) if extracted else None
    finished = datetime.now(timezone.utc)
    task_starts = [t.start_date for t in dag_run.get_task_instances() if t.start_date]
    started = min(task_starts) if task_starts else (dag_run.start_date or finished)

    row = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "logical_date": context["logical_date"],
        "status": "failed" if failed else "success",
        "started_at": started,
        "finished_at": finished,
        "duration_seconds": round((finished - started).total_seconds(), 2),
        "records_extracted": extracted,
        "records_loaded": ingestion.get("records_loaded"),
        "records_rejected": rejected,
        "rejection_rate_pct": rejection_pct,
        "dbt_models_ok": dbt_run.get("success"),
        "dbt_models_error": dbt_run.get("error"),
        "dbt_tests_pass": dbt_test.get("pass"),
        "dbt_tests_warn": dbt_test.get("warn"),
        "dbt_tests_fail": (dbt_test.get("fail") or 0) + (dbt_test.get("error") or 0) or None,
        "failed_tasks": failed,
        "error_message": ", ".join(ingestion.get("failed_sources", [])) or None,
    }

    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    # Make sure the table exists even if ingestion never ran (same DDL file it uses).
    hook.run((Path(PROJECT_ROOT) / "ingestion" / "sql" / "warehouse_ddl.sql").read_text())
    hook.run(UPSERT_SQL, parameters=row)
    print(f"Recorded pipeline run: {row}")

    if rejection_pct is not None and rejection_pct > REJECTION_ALERT_PCT:
        send_slack(
            f"*DataFlow data-quality warning*: {rejection_pct}% of records were quarantined "
            f"in run `{dag_run.run_id}` (threshold {REJECTION_ALERT_PCT}%). "
            "Check `quarantine.rejection_summary`.",
            emoji=":warning:",
        )
    if dbt_test.get("warn"):
        print(f"dbt reported {dbt_test['warn']} warning test(s); see schema test_failures.")


def fail_if_anything_failed(**context) -> None:
    raise AirflowException("One or more upstream tasks failed; marking this DAG run as failed.")


with DAG(
    dag_id=DAG_ID,
    description="Ingest multi-source e-commerce data, transform with dbt, test, and log metrics",
    doc_md=__doc__,
    schedule="0 2 * * *",  # daily at 02:00 UTC, processing the previous day
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # ingestion state (watermarks) assumes one run at a time
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["dataflow", "ingestion", "dbt"],
    params={
        "full_refresh": Param(False, type="boolean", description="Ignore watermarks; re-read everything"),
        "backfill_since": Param(
            "", type="string", description="Re-ingest from YYYY-MM-DD (empty = incremental)"
        ),
        "simulate_sources": Param(
            True, type="boolean", description="Demo: generate the day's source data first"
        ),
    },
    render_template_as_native_obj=False,
) as dag:

    simulate_source_data = BashOperator(
        task_id="simulate_source_data",
        bash_command=(
            "{% if params.simulate_sources %}"
            f'cd "{PROJECT_ROOT}" && "{PIPELINE_PYTHON}" -m data_generators.generate append '
            "--date {{ ds }} --init-if-missing"
            "{% else %}echo 'Source simulation disabled'{% endif %}"
        ),
        retries=1,
    )

    check_source_availability = BashOperator(
        task_id="check_source_availability",
        bash_command=f'cd "{PROJECT_ROOT}" && "{PIPELINE_PYTHON}" -m ingestion.healthcheck',
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )

    run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && "{PIPELINE_PYTHON}" -m ingestion.run --run-id "{{{{ run_id }}}}"'
            "{% if params.full_refresh %} --full-refresh{% endif %}"
            "{% if params.backfill_since %} --since {{ params.backfill_since }}{% endif %}"
        ),
        do_xcom_push=True,  # last stdout line = JSON summary
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt(
            f'"{DBT_BIN}" source freshness || echo "WARNING: source freshness check reported issues"\n'
            f'"{DBT_BIN}" snapshot && "{DBT_BIN}" run'
            "{% if params.full_refresh %} --full-refresh{% endif %}",
            "run",
        ),
        retries=1,  # model errors are rarely transient; one retry covers DB blips
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt(f'"{DBT_BIN}" test', "test"),
        retries=0,  # a failing data test is not transient; retrying won't fix the data
    )

    record_metrics = PythonOperator(
        task_id="log_run_metrics",
        python_callable=log_run_metrics,
        trigger_rule=TriggerRule.ALL_DONE,  # record failures too
        retries=1,
    )

    watcher = PythonOperator(
        task_id="watcher",
        python_callable=fail_if_anything_failed,
        trigger_rule=TriggerRule.ONE_FAILED,
        on_failure_callback=None,  # the real failing task already alerted
        email_on_failure=False,
        retries=0,
    )

    (
        simulate_source_data
        >> check_source_availability
        >> run_ingestion
        >> dbt_run
        >> dbt_test
        >> record_metrics
    )
    [simulate_source_data, check_source_availability, run_ingestion, dbt_run, dbt_test] >> watcher
