-- Customers should never be charged more than the order total.
{{ config(severity='warn') }}

select order_id, order_total, amount_paid
from {{ ref('fact_orders') }}
where amount_paid > order_total + 0.01
