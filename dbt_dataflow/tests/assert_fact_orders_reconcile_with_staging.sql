-- Reconciliation: the fact table must contain every order exactly once and the
-- same total amount as staging. Any difference means a join is losing or
-- duplicating rows.
with s as (
    select count(*) as n, coalesce(sum(total_amount), 0) as total from {{ ref('stg_orders') }}
),
f as (
    select count(*) as n, coalesce(sum(order_total), 0) as total from {{ ref('fact_orders') }}
)
select s.n as staging_orders, f.n as fact_orders, s.total as staging_total, f.total as fact_total
from s cross join f
where s.n <> f.n or abs(s.total - f.total) > 0.01
