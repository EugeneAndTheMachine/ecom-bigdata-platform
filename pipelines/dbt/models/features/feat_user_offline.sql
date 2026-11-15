-- Offline user features (RFM + behaviour), consumed BY PATH by Spark ML (segmentation)
{{ config(properties=gold_location('feat_user_offline')) }}

-- Anchored on ONE timeline (var ml_source); every window is bounded on both sides by as_of_date
-- so rows of the other timeline never leak in.
with asof as (
    select max(event_date) as as_of_date
    from {{ ref('stg_events') }}
    where {{ ml_source_predicate() }}
),

recent_events as (
    select e.*, asof.as_of_date
    from {{ ref('stg_events') }} as e
    cross join asof
    where e.event_date > asof.as_of_date - interval '{{ var("feature_window_days") }}' day
      and e.event_date <= asof.as_of_date
),

behaviour as (
    select
        user_id,
        count(distinct user_session)                         as sessions_30d,
        count(distinct if(event_date > as_of_date - interval '7' day, user_session)) as sessions_7d,
        count_if(event_type = 'view')                        as views_30d,
        count_if(event_type = 'cart')                        as carts_30d,
        count_if(event_type = 'purchase')                    as purchases_30d,
        count(distinct product_id)                           as distinct_products_30d
    from recent_events
    group by user_id
),

fav as (
    select user_id, max_by(category_l1, n) as fav_category_l1
    from (
        select user_id, category_l1, count(*) as n
        from recent_events
        where category_l1 <> 'unknown'
        group by 1, 2
    )
    group by user_id
),

rfm as (
    select
        user_id,
        max(order_date)  as last_order_date,
        count(*)         as frequency,
        sum(revenue)     as monetary
    from {{ ref('fact_orders') }}
    cross join asof
    where order_date <= asof.as_of_date
    group by user_id
),

sess as (
    select user_id, avg(duration_s) as avg_session_duration_s
    from {{ ref('stg_sessions') }}
    cross join asof
    where session_date > asof.as_of_date - interval '{{ var("feature_window_days") }}' day
      and session_date <= asof.as_of_date
    group by user_id
)

select
    u.user_id,
    asof.as_of_date,
    date_diff('day', r.last_order_date, asof.as_of_date)          as recency_days,
    coalesce(r.frequency, 0)                                      as frequency,
    coalesce(r.monetary, 0)                                       as monetary,
    coalesce(b.sessions_7d, 0)                                    as sessions_7d,
    coalesce(b.sessions_30d, 0)                                   as sessions_30d,
    coalesce(b.views_30d, 0)                                      as views_30d,
    coalesce(b.carts_30d, 0)                                      as carts_30d,
    coalesce(b.purchases_30d, 0)                                  as purchases_30d,
    coalesce(b.distinct_products_30d, 0)                          as distinct_products_30d,
    cast(b.purchases_30d as double) / nullif(b.carts_30d, 0)      as cart_to_purchase_rate_30d,
    f.fav_category_l1,
    s.avg_session_duration_s
from {{ ref('dim_user') }} as u
cross join asof
left join behaviour as b on b.user_id = u.user_id
left join fav       as f on f.user_id = u.user_id
left join rfm       as r on r.user_id = u.user_id
left join sess      as s on s.user_id = u.user_id
