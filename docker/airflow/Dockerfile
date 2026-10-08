FROM apache/airflow:2.10.5-python3.11

USER root
# git is needed for `dbt deps`
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Isolated venv for ingestion code + dbt.
# Airflow 2.x pins SQLAlchemy 1.4, but pandas 2.2 / our code use SQLAlchemy 2.x,
# so they must not share one Python environment.
RUN python -m venv /opt/pipeline_venv && chown -R airflow: /opt/pipeline_venv

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN /opt/pipeline_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
