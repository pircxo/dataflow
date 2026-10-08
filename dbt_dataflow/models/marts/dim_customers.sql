-- One row per real person (duplicate accounts merged), plus an "Unknown" member (-1)
-- so facts with orphan customer ids still join.
with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select
        canonical_customer_id,
        count(*)                                        as account_count,
        array_agg(customer_id order by customer_id)     as linked_customer_ids,
        max(updated_at)                                 as updated_at
    from customers
    group by 1
),

-- newest contact details across all linked accounts
latest_profile as (
    select distinct on (canonical_customer_id)
        canonical_customer_id, city, phone, country_code
    from customers
    order by canonical_customer_id, updated_at desc, customer_id desc
),

primary_account as (
    select * from customers where customer_id = canonical_customer_id
)

select
    p.customer_id                                   as customer_key,
    p.email,
    p.first_name,
    p.last_name,
    p.first_name || ' ' || p.last_name              as full_name,
    lp.city,
    lp.phone,
    lp.country_code,
    p.created_at                                    as signed_up_at,
    p.created_at::date                              as signup_date,
    a.account_count,
    a.account_count > 1                             as has_duplicate_accounts,
    a.linked_customer_ids,
    a.updated_at
from primary_account p
join accounts a       on a.canonical_customer_id = p.customer_id
join latest_profile lp on lp.canonical_customer_id = p.customer_id

union all

select
    -1, null, 'Unknown', 'Customer', 'Unknown Customer', null, null, null,
    null, null, 0, false, array[]::int[], null
