-- Runs automatically the FIRST time the warehouse volume is created.
CREATE SCHEMA IF NOT EXISTS raw;           -- validated data, as close to source as possible
CREATE SCHEMA IF NOT EXISTS quarantine;    -- rejected rows + reason, for debugging/replay
CREATE SCHEMA IF NOT EXISTS staging;       -- dbt: cleaned, renamed, typed
CREATE SCHEMA IF NOT EXISTS analytics;     -- dbt: star schema (dims + facts) + BI views
CREATE SCHEMA IF NOT EXISTS observability; -- pipeline run metrics, watermarks

COMMENT ON SCHEMA raw           IS 'Validated source data loaded by the Python ingestion engine';
COMMENT ON SCHEMA quarantine    IS 'Records that failed data-quality checks, with rejection reasons';
COMMENT ON SCHEMA staging       IS 'dbt staging models';
COMMENT ON SCHEMA analytics     IS 'dbt marts: dimensions, facts, BI views';
COMMENT ON SCHEMA observability IS 'Pipeline run logs, watermarks, data-quality metrics';
