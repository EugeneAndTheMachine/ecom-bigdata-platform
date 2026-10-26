select
    user_session,
    user_id,
    session_date,
    session_start,
    session_end,
    duration_s,
    n_events,
    n_views,
    n_carts,
    n_removes,
    n_purchases,
    revenue,
    cart_value,
    n_distinct_products,
    n_distinct_categories,
    main_category_l1,
    last_category_l1,
    first_purchase_time,
    converted,
    cart_abandoned
from {{ source('silver', 'sessions') }}
where not is_bot
