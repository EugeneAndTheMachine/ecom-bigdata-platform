{{ config(properties=gold_location('fact_orders')) }}

select
    order_id,
    min(user_id)          as user_id,
    min(date_key)         as date_key,
    min(order_date)       as order_date,
    min(purchased_at)     as order_time,
    count(*)              as n_items,
    count(distinct product_id) as n_distinct_products,
    sum(price)            as revenue
from {{ ref('fact_order_items') }}
group by order_id
