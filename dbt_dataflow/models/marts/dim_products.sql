-- Current product catalogue, plus an "UNKNOWN" member for order lines
-- that reference products missing from the catalogue.
select
    product_id                                      as product_key,
    product_name,
    category,
    brand,
    unit_cost,
    list_price,
    gross_margin_pct,
    currency_code,
    is_active,
    updated_at
from {{ ref('stg_products') }}

union all

select 'UNKNOWN', 'Unknown Product', 'Unknown', 'Unknown', null, null, null, null, false, null
