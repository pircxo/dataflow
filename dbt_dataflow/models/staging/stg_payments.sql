with source as (
    select * from {{ source('raw', 'payments') }}
)

select
    payment_id,
    order_id,
    amount,
    upper(trim(currency))                        as currency_code,
    lower(trim(status))                          as payment_status,
    lower(trim(method))                          as payment_method,
    failure_reason,
    created_at,
    created_at::date                             as payment_date,
    updated_at,
    lower(trim(status)) = 'succeeded'            as is_successful,
    lower(trim(status)) = 'failed'               as is_failed,
    lower(trim(status)) = 'refunded'             as is_refunded,
    _ingested_at
from source
