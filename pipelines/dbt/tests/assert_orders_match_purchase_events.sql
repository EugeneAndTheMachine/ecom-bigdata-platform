-- Singular test: order lines must equal non-bot purchase events (no loss in the star schema).
with a as (select count(*) as n from {{ ref('fact_order_items') }}),
     b as (select count(*) as n from {{ ref('stg_events') }} where event_type = 'purchase')
select a.n as order_items, b.n as purchase_events
from a cross join b
where a.n <> b.n
