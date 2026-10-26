-- Human traffic only. Bots stay in Silver (flagged) for audit & reconciliation.
select
    event_id,
    event_time,
    event_date,
    event_hour,
    event_type,
    product_id,
    category_id,
    category_code,
    category_l1,
    category_l2,
    category_l3,
    brand,
    price,
    user_id,
    user_session,
    _source as source_system
from {{ source('silver', 'events') }}
where not is_bot
