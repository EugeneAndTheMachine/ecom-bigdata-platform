-- Weekly interest per category with week-over-week growth (replaces Google Trends in REES46-only scope)
with weekly as (
    select
        date_trunc('week', event_date)      as week_start,
        category_l1,
        count_if(event_type = 'view')       as views,
        count_if(event_type = 'cart')       as carts,
        count_if(event_type = 'purchase')   as purchases,
        count(distinct user_id)             as active_users
    from {{ ref('fact_events') }}
    group by 1, 2
)

select
    week_start,
    category_l1,
    views,
    carts,
    purchases,
    active_users,
    lag(views) over (partition by category_l1 order by week_start) as views_prev_week,
    cast(views as double) / nullif(lag(views) over (partition by category_l1 order by week_start), 0) - 1
        as views_wow_growth,
    rank() over (partition by week_start order by views desc)      as views_rank_in_week
from weekly
