-- Observability check: if the latest ingestion run rejected more than 20% of
-- any entity's records, something upstream probably changed (schema, format).
{{ config(severity='warn') }}

with latest as (
    select distinct on (entity) entity, run_id, rows_extracted, rows_quarantined
    from {{ source('observability', 'ingestion_runs') }}
    where status = 'success' and rows_extracted > 0
    order by entity, finished_at desc
)
select entity, run_id, rows_extracted, rows_quarantined,
       round(100.0 * rows_quarantined / rows_extracted, 2) as rejection_rate_pct
from latest
where rows_quarantined > 0.20 * rows_extracted
