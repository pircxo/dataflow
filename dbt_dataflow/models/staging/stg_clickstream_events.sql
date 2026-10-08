-- Flattens the nested JSON "context" object into columns.
with source as (
    select * from {{ source('raw', 'clickstream_events') }}
)

select
    event_id,
    session_id,
    customer_id,
    lower(trim(event_type))                      as event_type,
    upper(trim(product_id))                      as product_id,
    page_url,
    event_ts,
    event_ts::date                               as event_date,
    context ->> 'device'                         as device_type,
    context ->> 'browser'                        as browser,
    upper(context ->> 'country')                 as country_code,
    _ingested_at
from source
