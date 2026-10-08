-- Failed-payment rate per day and payment method, plus an 'ALL' row per day.
-- Only settled attempts count (pending ones are excluded until they resolve).
with attempts as (
    select
        p.*,
        d.date
    from {{ ref('fact_payments') }} p
    join {{ ref('dim_date') }} d on d.date_key = p.payment_date_key
    where p.payment_status in ('succeeded', 'failed', 'refunded')
)

select
    date,
    coalesce(payment_method, 'ALL')                         as payment_method,
    count(*)                                                as settled_attempts,
    count(*) filter (where is_failed)                       as failed_attempts,
    count(*) filter (where not is_failed)                   as successful_attempts,
    round(100.0 * count(*) filter (where is_failed) / count(*), 2) as failure_rate_pct,
    sum(amount) filter (where is_failed)                    as failed_amount,
    mode() within group (order by failure_reason)
        filter (where is_failed)                            as top_failure_reason
from attempts
group by grouping sets ((date, payment_method), (date))
