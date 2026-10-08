-- One row of headline numbers for KPI cards (last 30 days vs the 30 days before).
with as_of as (
    select max(order_date) as d from {{ ref('fact_orders') }}
),

periods as (
    select
        f.*,
        case
            when f.order_date >  a.d - 30 then 'current'
            when f.order_date >  a.d - 60 then 'previous'
        end as period
    from {{ ref('fact_orders') }} f cross join as_of a
),

orders_kpis as (
    select
        sum(net_revenue) filter (where period = 'current')                as net_revenue_30d,
        sum(net_revenue) filter (where period = 'previous')               as net_revenue_prev_30d,
        count(*) filter (where period = 'current')                        as orders_30d,
        count(*) filter (where period = 'previous')                       as orders_prev_30d,
        count(distinct customer_key) filter (where period = 'current' and customer_key <> -1) as active_customers_30d
    from periods
),

payment_kpis as (
    select round(100.0 * sum(failed_attempts) / nullif(sum(settled_attempts), 0), 2) as failed_payment_rate_30d
    from {{ ref('bi_payment_failure_rate') }}
    where payment_method = 'ALL' and date > (select d from as_of) - 30
),

last_run as (
    select status as last_pipeline_status, finished_at as last_pipeline_run_at,
           rejection_rate_pct as last_run_rejection_pct
    from {{ source('observability', 'pipeline_runs') }}
    order by recorded_at desc
    limit 1
)

select
    (select d from as_of)                                               as as_of_date,
    o.net_revenue_30d,
    o.net_revenue_prev_30d,
    round(100.0 * (o.net_revenue_30d - o.net_revenue_prev_30d) / nullif(o.net_revenue_prev_30d, 0), 2)
                                                                        as net_revenue_change_pct,
    o.orders_30d,
    o.orders_prev_30d,
    round(o.net_revenue_30d / nullif(o.orders_30d, 0), 2)               as revenue_per_order_30d,
    o.active_customers_30d,
    (select round(avg(lifetime_value), 2) from {{ ref('bi_customer_ltv') }}) as avg_customer_ltv,
    p.failed_payment_rate_30d,
    r.last_pipeline_status,
    r.last_pipeline_run_at,
    r.last_run_rejection_pct
from orders_kpis o
cross join payment_kpis p
left join last_run r on true
