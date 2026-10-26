-- Training set for the purchase-intent model: sessions with >= 1 add-to-cart.
-- Columns MUST match libs/ecom_common/features.py::INTENT_FEATURES (+ label + split key).
{{ config(properties=gold_location('feat_session_training')) }}

select
    user_session,
    user_id,
    session_start,                                                     -- time-based split key
    cast(n_views as double)                                            as n_views,
    cast(n_carts as double)                                            as n_carts,
    cast(greatest(n_carts - n_removes, 0) as double)                   as cart_items,
    cast(cart_value as double)                                         as cart_value,
    cast(least(n_distinct_categories, 5) as double)                    as n_categories,
    -- stop the clock at the first purchase -> no leakage of post-purchase browsing time
    cast(date_diff('millisecond', session_start, coalesce(first_purchase_time, session_end)) as double) / 1000
                                                                       as duration_s,
    cast(hour(session_start) as double)                                as hour_of_day,
    cast(day_of_week(session_start) as double)                         as day_of_week,
    converted
from {{ ref('stg_sessions') }}
where n_carts > 0
