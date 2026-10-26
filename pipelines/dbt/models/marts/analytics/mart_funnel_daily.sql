-- Q1 (batch reference): event funnel + session funnel per day x category
with ev as (
    select
        event_date as date_day,
        category_l1,
        count_if(event_type = 'view')     as views,
        count_if(event_type = 'cart')     as carts,
        count_if(event_type = 'purchase') as purchases,
        sum(if(event_type = 'purchase', price, 0)) as revenue
    from {{ ref('fact_events') }}
    group by 1, 2
),

se as (
    select
        session_date as date_day,
        main_category_l1 as category_l1,
        count(*)                       as sessions,
        count_if(n_carts > 0)          as sessions_with_cart,
        count_if(converted)            as converted_sessions,
        count_if(cart_abandoned)       as abandoned_sessions
    from {{ ref('fact_sessions') }}
    group by 1, 2
)

select
    ev.date_day,
    ev.category_l1,
    ev.views,
    ev.carts,
    ev.purchases,
    ev.revenue,
    cast(ev.carts as double) / nullif(ev.views, 0)            as view_to_cart_rate,
    cast(ev.purchases as double) / nullif(ev.carts, 0)        as cart_to_purchase_rate,
    cast(ev.purchases as double) / nullif(ev.views, 0)        as view_to_purchase_rate,
    se.sessions,
    se.sessions_with_cart,
    se.converted_sessions,
    se.abandoned_sessions,
    cast(se.converted_sessions as double) / nullif(se.sessions, 0)          as session_conversion_rate,
    cast(se.abandoned_sessions as double) / nullif(se.sessions_with_cart, 0) as cart_abandonment_rate
from ev
left join se on se.date_day = ev.date_day and se.category_l1 = ev.category_l1
