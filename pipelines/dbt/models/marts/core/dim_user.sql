-- One row per (non-bot) user. Segment comes from the ML job (ml.user_segments) when it exists.
{% set seg = optional_relation('ml', 'user_segments') %}

with users as (
    select
        user_id,
        min(event_time)                          as first_seen_at,
        max(event_time)                          as last_seen_at,
        count(distinct user_session)             as lifetime_sessions,
        count_if(event_type = 'purchase')        as lifetime_purchases,
        sum(if(event_type = 'purchase', price, 0)) as lifetime_revenue
    from {{ ref('stg_events') }}
    group by user_id
)

select
    users.*,
{% if seg %}
    s.segment,
    s.segment_label,
    s.churn_risk
{% else %}
    cast(null as integer) as segment,
    cast(null as varchar) as segment_label,
    cast(null as boolean) as churn_risk
{% endif %}
from users
{% if seg %}
left join {{ seg }} as s on s.user_id = users.user_id
{% endif %}
