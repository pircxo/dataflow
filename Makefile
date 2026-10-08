# Shortcuts. Run `make help` to see them all.
# Pipeline commands run INSIDE the Airflow container, so you don't need Python locally.

COMPOSE  := docker compose
IN_AF    := $(COMPOSE) exec -T airflow-scheduler bash -c
PY       := cd /opt/airflow && /opt/pipeline_venv/bin/python
DBT      := cd /opt/airflow/dbt_dataflow && /opt/pipeline_venv/bin/dbt

.PHONY: help env up down reset logs seed next-day ingest dbt-build dbt-docs trigger psql quarantine runs lint test

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env:  ## Create .env from the example (if missing)
	@test -f .env || cp .env.example .env

up: env  ## Build and start everything (Airflow UI: http://localhost:8080)
	$(COMPOSE) up -d --build

down:  ## Stop everything (keeps data)
	$(COMPOSE) down

reset:  ## Stop everything and DELETE all data (databases + generated files)
	$(COMPOSE) down -v
	find data/generated -mindepth 1 ! -name .gitkeep -exec rm -rf {} +

logs:  ## Follow Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

seed:  ## Generate 60 days of messy history in the sources (wipes sources)
	$(IN_AF) "$(PY) -m data_generators.generate init --days 60"

next-day:  ## Simulate the next day in the sources
	$(IN_AF) "$(PY) -m data_generators.generate append"

ingest:  ## Run ingestion once (incremental)
	$(IN_AF) "$(PY) -m ingestion.run"

dbt-build:  ## dbt snapshot + models + tests
	$(IN_AF) "$(DBT) build"

dbt-docs:  ## Generate dbt docs (open dbt_dataflow/target/index.html)
	$(IN_AF) "$(DBT) docs generate"

trigger:  ## Unpause and trigger the Airflow DAG
	$(COMPOSE) exec -T airflow-scheduler airflow dags unpause dataflow_pipeline
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger dataflow_pipeline

psql:  ## Open psql on the warehouse
	$(COMPOSE) exec warehouse-db psql -U dataflow -d warehouse

quarantine:  ## Show why records were rejected
	$(COMPOSE) exec -T warehouse-db psql -U dataflow -d warehouse -c "SELECT * FROM quarantine.rejection_summary ORDER BY entity, records DESC"

runs:  ## Show the last pipeline runs
	$(COMPOSE) exec -T warehouse-db psql -U dataflow -d warehouse -c "SELECT run_id, status, duration_seconds, records_extracted, records_rejected, rejection_rate_pct, dbt_tests_warn, dbt_tests_fail, failed_tasks FROM observability.pipeline_runs ORDER BY recorded_at DESC LIMIT 10"

lint:  ## black + flake8 (local venv)
	black --check . && flake8 .

test:  ## pytest (local venv)
	pytest -q
