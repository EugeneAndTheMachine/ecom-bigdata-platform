-- Grain: one event. Incremental MERGE on event_id with a look-back window so late events
-- merged into Silver after the fact are picked up.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    on_schema_change='append_new_columns',
    properties={"partitioned_by": "ARRAY['event_date']"}
) }}

select
    e.event_id,
    cast(date_format(e.event_date, '%Y%m%d') as integer) as date_key,
    e.event_time,
    e.user_id,
    e.user_session,
    e.product_id,
    p.product_key,                                -- as-of join on the SCD2 dimension
    e.category_l1,
    e.brand,
    e.event_type,
    e.price,
    e.event_date
from {{ ref('stg_events') }} as e
left join {{ ref('dim_product') }} as p
    on  p.product_id = e.product_id
    and e.event_time >= p.valid_from
    and (p.valid_to is null or e.event_time < p.valid_to)
{% if is_incremental() %}
where e.event_date >= (
    select coalesce(max(event_date), date '1970-01-01') - interval '{{ var("lookback_days") }}' day
    from {{ this }}
)
{% endif %}
