-- Grain: one row per order line. Used for product / category analysis.
-- Lines whose order header was quarantined are excluded (inner join) and
-- reported by the stg_order_items relationship test.
with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, customer_key, order_date_key, order_status from {{ ref('fact_orders') }}
),

products as (
    select product_key, unit_cost from {{ ref('dim_products') }}
)

select
    i.order_item_id,
    i.order_id,
    o.customer_key,
    o.order_date_key,
    coalesce(p.product_key, 'UNKNOWN')              as product_key,
    i.product_id                                    as source_product_id,
    o.order_status,
    i.quantity,
    i.unit_price,
    i.line_amount,
    -- NOTE: uses the CURRENT unit cost. For exact historical margins, join a
    -- product snapshot (SCD type 2) as of the order date instead.
    round(i.quantity * p.unit_cost, 2)              as line_cost,
    i.line_amount - round(i.quantity * p.unit_cost, 2) as line_margin
from items i
join orders o         on o.order_id = i.order_id
left join products p  on p.product_key = i.product_id
