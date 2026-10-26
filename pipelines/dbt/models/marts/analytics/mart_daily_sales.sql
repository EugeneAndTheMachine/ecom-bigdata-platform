-- Q5: revenue, orders, AOV by day x category x brand (+ calendar context)
select
    d.date_day,
    d.year_month,
    d.iso_day_of_week,
    d.is_weekend,
    d.is_holiday,
    d.holiday_name,
    i.category_l1,
    i.brand,
    count(distinct i.order_id)                              as orders,
    count(*)                                                as items,
    count(distinct i.user_id)                               as buyers,
    sum(i.price)                                            as revenue,
    sum(i.price) / nullif(count(distinct i.order_id), 0)    as aov,
    avg(i.price)                                            as avg_item_price
from {{ ref('fact_order_items') }} as i
join {{ ref('dim_date') }} as d on d.date_key = i.date_key
group by 1, 2, 3, 4, 5, 6, 7, 8
