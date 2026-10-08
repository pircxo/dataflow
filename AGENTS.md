# Guide for AI coding assistants (Codex, Claude Code)

DataFlow is a portfolio data pipeline for Junior Data Engineer roles.
Read README.md first: it explains the architecture, the runbook and design choices.

## Map
- `data_generators/` – fake shop that writes messy data ON PURPOSE (bad rows are a feature, not a bug)
- `api/mock_payments/` – FastAPI payments API that fails randomly ON PURPOSE (tests retries)
- `ingestion/` – Python: extract -> validate -> load `raw` / `quarantine`, watermarks, retries
- `dbt_dataflow/` – staging views, star schema (marts), BI views (models/bi), tests
- `airflow/dags/` – daily DAG + alert/metrics helpers
- `.github/workflows/ci.yml` – lint + full pipeline run on every PR

## Rules
- History: one commit per milestone (`git log --oneline`, `git show <sha>`). Review milestone by milestone.
- Explain things in simple, plain English. The owner is learning; say WHY, not only WHAT.
- Do not change files until the owner agrees. Propose changes first.
- Keep loads idempotent and the watermark in the same transaction as the data.
- Run before saying something works: `black --check . && flake8 . && pytest -q`
  and `dbt build --project-dir dbt_dataflow` (needs Postgres: `make up`).
