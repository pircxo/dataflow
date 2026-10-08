-- Customer lifetime value and simple RFM-style segments, one row per customer.
-- LTV = all net revenue to date (paid, not cancelled/returned).
with orders as (
    select * from {{ ref('fact_orders') }} where customer_key <> -1
),

as_of as (
    select max(order_date) as as_of_date from orders
),

per_customer as (
    select
        customer_key,
        min(order_date)                             as first_order_date,
        max(order_date)                             as last_order_date,
        count(*)                                    as orders,
        count(*) filter (where net_revenue > 0)     as revenue_orders,
        sum(net_revenue)                            as lifetime_value,
        sum(amount_refunded)                        as lifetime_refunds
    from orders
    group by 1
)

select
    c.customer_key,
    c.full_name,
    c.email,
    c.country_code,
    c.signup_date,
    pc.first_order_date,
    pc.last_order_date,
    pc.orders,
    pc.lifetime_value,
    round(pc.lifetime_value / nullif(pc.revenue_orders, 0), 2) as avg_order_value,
    pc.lifetime_refunds,
    a.as_of_date - pc.last_order_date                         as days_since_last_order,
    case
        when pc.orders = 1 and a.as_of_date - pc.first_order_date <= 30 then 'New'
        when a.as_of_date - pc.last_order_date <= 30                    then 'Active'
        when a.as_of_date - pc.last_order_date <= 90                    then 'At risk'
        else 'Lapsed'
    end                                                       as customer_segment,
    ntile(4) over (order by pc.lifetime_value)                as ltv_quartile   -- 4 = top 25%
from per_customer pc
join {{ ref('dim_customers') }} c on c.customer_key = pc.customer_key
cross join as_of a
