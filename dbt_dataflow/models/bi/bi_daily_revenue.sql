-- Revenue per day, including days with no orders (calendar-driven).
-- Power BI: line chart of net_revenue + net_revenue_7d_avg over date.
with orders as (
    select * from {{ ref('fact_orders') }}
),

bounds as (
    select min(order_date) as first_day, max(order_date) as last_day from orders
),

daily as (
    select
        order_date,
        count(*)                                            as orders_placed,
        count(*) filter (where is_paid)                     as orders_paid,
        count(*) filter (where order_status = 'cancelled')  as orders_cancelled,
        count(*) filter (where order_status = 'returned')   as orders_returned,
        sum(units)                                          as units_sold,
        sum(order_total)                                    as gross_order_value,
        sum(net_revenue)                                    as net_revenue,
        sum(amount_refunded)                                as refunds
    from orders
    group by 1
)

select
    d.date,
    d.date_key,
    d.day_name,
    d.is_weekend,
    d.year_month,
    coalesce(x.orders_placed, 0)                            as orders_placed,
    coalesce(x.orders_paid, 0)                              as orders_paid,
    coalesce(x.orders_cancelled, 0)                         as orders_cancelled,
    coalesce(x.orders_returned, 0)                          as orders_returned,
    coalesce(x.units_sold, 0)                               as units_sold,
    coalesce(x.gross_order_value, 0)                        as gross_order_value,
    coalesce(x.net_revenue, 0)                              as net_revenue,
    coalesce(x.refunds, 0)                                  as refunds,
    round(coalesce(x.net_revenue, 0) / nullif(x.orders_paid, 0), 2) as avg_order_value,
    round(avg(coalesce(x.net_revenue, 0)) over (
        order by d.date rows between 6 preceding and current row
    ), 2)                                                   as net_revenue_7d_avg,
    sum(coalesce(x.net_revenue, 0)) over (
        partition by d.year_month order by d.date
    )                                                       as net_revenue_month_to_date
from {{ ref('dim_date') }} d
cross join bounds b
left join daily x on x.order_date = d.date
where d.date between b.first_day and b.last_day
