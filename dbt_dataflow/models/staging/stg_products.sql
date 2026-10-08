with source as (
    select * from {{ source('raw', 'products') }}
)

select
    upper(trim(product_id))                      as product_id,
    trim(product_name)                           as product_name,
    initcap(trim(category))                      as category,
    trim(brand)                                  as brand,
    unit_cost,
    list_price,
    case
        when list_price > 0 then round((list_price - unit_cost) / list_price * 100, 2)
    end                                          as gross_margin_pct,
    upper(trim(currency))                        as currency_code,
    coalesce(is_active, true)                    as is_active,
    updated_at,
    _ingested_at
from source
