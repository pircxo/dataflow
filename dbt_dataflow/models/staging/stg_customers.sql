-- One row per customer account, cleaned.
-- Identity resolution: people who signed up twice with the same email (any letter case)
-- are linked to their FIRST account via canonical_customer_id.
with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id,
        lower(trim(email))                       as email,
        initcap(trim(first_name))                as first_name,
        initcap(trim(last_name))                 as last_name,
        nullif(trim(phone), '')                  as phone,
        nullif(trim(city), '')                   as city,
        upper(trim(country))                     as country_code,
        created_at,
        updated_at,
        _ingested_at
    from source
)

select
    *,
    first_value(customer_id) over (
        partition by email
        order by created_at, customer_id
    )                                            as canonical_customer_id
from cleaned
