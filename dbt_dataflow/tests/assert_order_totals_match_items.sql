-- Order header total should equal the sum of its lines.
-- WARN only: the source system has known rounding/discount bugs, and lines
-- that were quarantined also cause gaps. Investigate if the count jumps.
{{ config(severity='warn') }}

select order_id, order_total, items_amount, order_total - items_amount as difference
from {{ ref('fact_orders') }}
where has_total_mismatch
