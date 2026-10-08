-- SCD Type 2 history of customer details: every change of city / phone / email
-- becomes a new row with dbt_valid_from / dbt_valid_to.
-- Snapshots read the SOURCE (dbt best practice), so they never depend on a model
-- having been built first and capture changes exactly as they arrived.
{% snapshot snap_customers %}
{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select
    customer_id,
    lower(trim(email))  as email,
    first_name,
    last_name,
    phone,
    city,
    upper(trim(country)) as country_code,
    updated_at
from {{ source('raw', 'customers') }}
{% endsnapshot %}
