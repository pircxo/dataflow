-- Calendar dimension. Range comes from project vars.
with days as (
    select generate_series(
        '{{ var("date_spine_start") }}'::date,
        '{{ var("date_spine_end") }}'::date,
        interval '1 day'
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int            as date_key,
    date_day                                      as date,
    extract(isodow from date_day)::int            as day_of_week,      -- 1 = Monday
    trim(to_char(date_day, 'Day'))                as day_name,
    extract(isodow from date_day) in (6, 7)       as is_weekend,
    date_trunc('week', date_day)::date            as week_start_date,
    extract(day from date_day)::int               as day_of_month,
    extract(month from date_day)::int             as month_number,
    trim(to_char(date_day, 'Month'))              as month_name,
    to_char(date_day, 'YYYY-MM')                  as year_month,
    extract(quarter from date_day)::int           as quarter_number,
    extract(year from date_day)::int              as year_number
from days
