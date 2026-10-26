-- REES46 has no order table. Business rule (documented in IMPLEMENTATION_GUIDE §8.4 + ADR-0003):
--   order = all purchase events of one user_session on one calendar day,
--   order line = one purchase event.
select
    e.user_session || '#' || cast(e.event_date as varchar) as order_id,
    e.event_id                                              as order_item_id,
    e.date_key,
    e.event_date                                            as order_date,
    e.event_time                                            as purchased_at,
    e.user_id,
    e.product_id,
    e.product_key,
    e.category_l1,
    e.brand,
    e.price
from {{ ref('fact_events') }} as e
where e.event_type = 'purchase'
