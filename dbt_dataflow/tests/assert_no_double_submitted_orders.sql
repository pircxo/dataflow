-- Same customer, same timestamp, same amount, different order ids =
-- a double-clicked "Place order" button. WARN: business must decide which to cancel.
{{ config(severity='warn') }}

select customer_id, order_ts, total_amount, array_agg(order_id order by order_id) as order_ids
from {{ ref('stg_orders') }}
group by customer_id, order_ts, total_amount
having count(*) > 1
