-- Grain: one row per payment attempt.
-- INCREMENTAL: each run only re-processes payments updated in the last N days
-- (var incremental_lookback_days) and merges them by payment_id. A payment's own
-- updated_at changes whenever its status changes, so this filter is reliable here.
{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

with payments as (
    select
        *,
        -- computed over ALL payments of the order, before the incremental filter
        row_number() over (partition by order_id order by created_at, payment_id) as attempt_number
    from {{ ref('stg_payments') }}
),

orders as (
    select order_id, customer_key from {{ ref('fact_orders') }}
)

select
    p.payment_id,
    p.order_id,
    coalesce(o.customer_key, -1)                    as customer_key,
    o.order_id is not null                          as has_known_order,
    to_char(p.payment_date, 'YYYYMMDD')::int        as payment_date_key,
    p.created_at,
    p.updated_at,
    p.payment_status,
    p.payment_method,
    p.failure_reason,
    p.amount,
    p.currency_code,
    p.is_successful,
    p.is_failed,
    p.is_refunded,
    p.attempt_number,
    current_timestamp                               as dbt_loaded_at
from payments p
left join orders o on o.order_id = p.order_id

{% if is_incremental() %}
where p.updated_at >= (
    select coalesce(max(updated_at), '1900-01-01'::timestamptz) from {{ this }}
) - interval '{{ var("incremental_lookback_days") }} days'
{% endif %}
