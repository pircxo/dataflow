-- Orders per day split by status and by new vs returning customers.
with orders as (
    select
        f.*,
        -- first order of this customer (Unknown customers are never "new")
        f.customer_key <> -1
            and f.order_ts = min(f.order_ts) over (partition by f.customer_key) as is_first_order
    from {{ ref('fact_orders') }} f
)

select
    o.order_date                                                as date,
    d.day_name,
    d.is_weekend,
    count(*)                                                    as total_orders,
    count(*) filter (where o.is_first_order)                    as orders_new_customers,
    count(*) filter (where not o.is_first_order)                as orders_returning_customers,
    count(*) filter (where o.order_status = 'placed')           as status_placed,
    count(*) filter (where o.order_status = 'paid')             as status_paid,
    count(*) filter (where o.order_status = 'shipped')          as status_shipped,
    count(*) filter (where o.order_status = 'delivered')        as status_delivered,
    count(*) filter (where o.order_status = 'cancelled')        as status_cancelled,
    count(*) filter (where o.order_status = 'returned')         as status_returned,
    round(avg(o.item_lines), 2)                                 as avg_lines_per_order,
    count(*) filter (where o.has_total_mismatch)                as orders_with_dq_issues
from orders o
join {{ ref('dim_date') }} d on d.date_key = o.order_date_key
group by 1, 2, 3
