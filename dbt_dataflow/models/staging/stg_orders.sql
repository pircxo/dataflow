with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    lower(trim(order_status))                    as order_status,
    order_ts,
    order_ts::date                               as order_date,
    nullif(trim(shipping_city), '')              as shipping_city,
    total_amount,
    upper(trim(currency))                        as currency_code,
    lower(trim(order_status)) in ('cancelled', 'returned') as is_cancelled_or_returned,
    updated_at,
    _ingested_at
from source
