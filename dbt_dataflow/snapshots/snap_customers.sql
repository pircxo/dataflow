-- SCD Type 2 history of customer details: every change of city / phone / email
-- becomes a new row with dbt_valid_from / dbt_valid_to.
{% snapshot snap_customers %}
{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select customer_id, email, first_name, last_name, phone, city, country_code, updated_at
from {{ ref('stg_customers') }}
{% endsnapshot %}
