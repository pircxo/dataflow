-- Monthly revenue, units and margin per product category.
-- Power BI: stacked column chart (year_month × category) or matrix.
select
    d.year_month,
    p.category,
    count(distinct i.order_id)                  as orders,
    sum(i.quantity)                             as units_sold,
    sum(i.line_amount)                          as revenue,
    sum(i.line_margin)                          as gross_margin,
    round(100.0 * sum(i.line_margin) / nullif(sum(i.line_amount), 0), 2) as gross_margin_pct
from {{ ref('fact_order_items') }} i
join {{ ref('dim_products') }} p on p.product_key = i.product_key
join {{ ref('dim_date') }} d     on d.date_key = i.order_date_key
where i.order_status not in ('cancelled', 'returned')
group by 1, 2
