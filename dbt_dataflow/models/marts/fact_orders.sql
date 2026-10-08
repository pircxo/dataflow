-- Grain: one row per order.
-- Full rebuild (table): an order's numbers change when its items OR payments change,
-- so a simple updated_at filter on orders would miss updates. Small table -> rebuild is cheap.
with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select customer_id, canonical_customer_id from {{ ref('stg_customers') }}
),

items as (
    select
        order_id,
        count(*)                                    as item_lines,
        sum(quantity)                               as units,
        sum(line_amount)                            as items_amount,
        max(updated_at)                             as items_updated_at
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select
        order_id,
        count(*)                                    as payment_attempts,
        count(*) filter (where is_failed)           as failed_payment_attempts,
        coalesce(sum(amount) filter (where is_successful), 0) as amount_paid,
        coalesce(sum(amount) filter (where is_refunded), 0)   as amount_refunded,
        bool_or(is_successful)                      as has_successful_payment,
        max(updated_at)                             as payments_updated_at
    from {{ ref('stg_payments') }}
    group by 1
),

latest_payment as (
    select distinct on (order_id) order_id, payment_status
    from {{ ref('stg_payments') }}
    order by order_id, created_at desc, updated_at desc
)

select
    o.order_id,
    coalesce(c.canonical_customer_id, -1)           as customer_key,
    o.customer_id                                   as source_customer_id,
    to_char(o.order_date, 'YYYYMMDD')::int          as order_date_key,
    o.order_ts,
    o.order_date,
    o.order_status,
    o.shipping_city,
    o.currency_code,
    coalesce(i.item_lines, 0)                       as item_lines,
    coalesce(i.units, 0)                            as units,
    o.total_amount                                  as order_total,
    coalesce(i.items_amount, 0)                     as items_amount,
    abs(o.total_amount - coalesce(i.items_amount, 0)) > 0.01 as has_total_mismatch,
    coalesce(p.payment_attempts, 0)                 as payment_attempts,
    coalesce(p.failed_payment_attempts, 0)          as failed_payment_attempts,
    coalesce(p.amount_paid, 0)                      as amount_paid,
    coalesce(p.amount_refunded, 0)                  as amount_refunded,
    coalesce(lp.payment_status, 'no_payment')       as latest_payment_status,
    coalesce(p.has_successful_payment, false)       as is_paid,
    -- Revenue we keep: paid, and not cancelled / returned.
    case
        when coalesce(p.has_successful_payment, false) and not o.is_cancelled_or_returned
            then o.total_amount
        else 0
    end                                             as net_revenue,
    greatest(o.updated_at, i.items_updated_at, p.payments_updated_at) as last_updated_at
from orders o
left join customers c       on c.customer_id = o.customer_id
left join items i           on i.order_id = o.order_id
left join payments p        on p.order_id = o.order_id
left join latest_payment lp on lp.order_id = o.order_id
