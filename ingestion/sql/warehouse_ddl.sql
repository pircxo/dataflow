-- Warehouse objects owned by the ingestion engine. Idempotent: runs on every start.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS observability;

-- ---------------------------------------------------------------------------
-- RAW: validated records, typed, one row per business key (latest version).
-- Every table carries the same lineage columns:
--   _record_hash  md5 of business columns -> skip no-op updates
--   _source       where the row came from (table, file name, API endpoint)
--   _run_id       which pipeline run loaded / last changed it
--   _ingested_at  when it was loaded / last changed
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id   integer PRIMARY KEY,
    email         text,
    first_name    text,
    last_name     text,
    phone         text,
    city          text,
    country       text,
    created_at    timestamptz,
    updated_at    timestamptz NOT NULL,
    _record_hash  text NOT NULL,
    _source       text,
    _run_id       text,
    _ingested_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id      integer PRIMARY KEY,
    customer_id   integer,
    order_status  text,
    order_ts      timestamptz,
    shipping_city text,
    total_amount  numeric(12, 2),
    currency      text,
    updated_at    timestamptz NOT NULL,
    _record_hash  text NOT NULL,
    _source       text,
    _run_id       text,
    _ingested_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id integer PRIMARY KEY,
    order_id      integer,
    product_id    text,
    quantity      integer,
    unit_price    numeric(12, 2),
    updated_at    timestamptz NOT NULL,
    _record_hash  text NOT NULL,
    _source       text,
    _run_id       text,
    _ingested_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id    text PRIMARY KEY,
    product_name  text,
    category      text,
    brand         text,
    unit_cost     numeric(12, 2),
    list_price    numeric(12, 2),
    currency      text,
    is_active     boolean,
    updated_at    timestamptz NOT NULL,
    _record_hash  text NOT NULL,
    _source       text,
    _run_id       text,
    _ingested_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id     text PRIMARY KEY,
    order_id       integer,
    amount         numeric(12, 2),
    currency       text,
    status         text,
    method         text,
    failure_reason text,
    created_at     timestamptz,
    updated_at     timestamptz NOT NULL,
    _record_hash   text NOT NULL,
    _source        text,
    _run_id        text,
    _ingested_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.clickstream_events (
    event_id      text PRIMARY KEY,
    session_id    text,
    customer_id   integer,
    event_type    text,
    product_id    text,
    page_url      text,
    event_ts      timestamptz NOT NULL,
    context       jsonb,
    _record_hash  text NOT NULL,
    _source       text,
    _run_id       text,
    _ingested_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- QUARANTINE: every rejected record, exactly as received, with all reasons.
-- Idempotent: the same bad record seen again only bumps times_seen.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine.rejected_records (
    id             bigserial PRIMARY KEY,
    entity         text        NOT NULL,
    record_key     text,
    reasons        text[]      NOT NULL,
    payload        jsonb       NOT NULL,
    payload_hash   text        NOT NULL,
    source         text,
    run_id         text,
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now(),
    times_seen     integer     NOT NULL DEFAULT 1,
    resolved_at    timestamptz,
    UNIQUE (entity, payload_hash)
);
CREATE INDEX IF NOT EXISTS ix_rejected_entity_seen ON quarantine.rejected_records (entity, last_seen_at);

CREATE OR REPLACE VIEW quarantine.rejection_summary AS
SELECT entity,
       reason,
       count(*)                                  AS records,
       count(*) FILTER (WHERE resolved_at IS NULL) AS open_records,
       max(last_seen_at)                         AS last_seen_at
FROM quarantine.rejected_records, unnest(reasons) AS reason
GROUP BY entity, reason;

-- ---------------------------------------------------------------------------
-- OBSERVABILITY / STATE
-- ---------------------------------------------------------------------------
-- High-water marks for incremental (cursor-based) sources.
CREATE TABLE IF NOT EXISTS observability.ingestion_watermarks (
    source_name  text PRIMARY KEY,
    watermark    timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    run_id       text
);

-- Files already loaded (file-based sources). Checksum lets a changed file be reloaded.
CREATE TABLE IF NOT EXISTS observability.processed_files (
    source_name   text NOT NULL,
    file_name     text NOT NULL,
    file_checksum text NOT NULL,
    rows_read     integer NOT NULL,
    processed_at  timestamptz NOT NULL DEFAULT now(),
    run_id        text,
    PRIMARY KEY (source_name, file_name)
);

-- One row per entity per ingestion run.
CREATE TABLE IF NOT EXISTS observability.ingestion_runs (
    id                bigserial PRIMARY KEY,
    run_id            text        NOT NULL,
    entity            text        NOT NULL,
    status            text        NOT NULL,   -- success | failed
    started_at        timestamptz NOT NULL,
    finished_at       timestamptz NOT NULL,
    duration_seconds  numeric(10, 2),
    rows_extracted    integer DEFAULT 0,
    rows_valid        integer DEFAULT 0,
    rows_quarantined  integer DEFAULT 0,
    rows_deduplicated integer DEFAULT 0,
    rows_inserted     integer DEFAULT 0,
    rows_updated      integer DEFAULT 0,
    rows_unchanged    integer DEFAULT 0,
    watermark_before  timestamptz,
    watermark_after   timestamptz,
    files_processed   integer DEFAULT 0,
    error_message     text
);
CREATE INDEX IF NOT EXISTS ix_ingestion_runs_run_id ON observability.ingestion_runs (run_id);

-- One row per end-to-end pipeline run (written by the Airflow DAG).
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id               text PRIMARY KEY,
    dag_id               text,
    logical_date         timestamptz,
    status               text NOT NULL,      -- success | failed
    started_at           timestamptz,
    finished_at          timestamptz,
    duration_seconds     numeric(10, 2),
    records_extracted    integer,
    records_loaded       integer,
    records_rejected     integer,
    rejection_rate_pct   numeric(6, 2),
    dbt_models_ok        integer,
    dbt_models_error     integer,
    dbt_tests_pass       integer,
    dbt_tests_warn       integer,
    dbt_tests_fail       integer,
    failed_tasks         text[],
    error_message        text,
    recorded_at          timestamptz NOT NULL DEFAULT now()
);
