-- Q6: segment sizes and value (segment is NULL until ml_weekly ran once)
select
    coalesce(segment_label, 'unscored')           as segment_label,
    count(*)                                      as users,
    sum(lifetime_revenue)                         as revenue,
    avg(lifetime_revenue)                         as avg_revenue_per_user,
    avg(lifetime_purchases)                       as avg_purchases_per_user,
    count_if(coalesce(churn_risk, false))         as users_at_churn_risk
from {{ ref('dim_user') }}
group by 1
