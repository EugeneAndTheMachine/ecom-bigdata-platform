-- Q8 (REES46 scope): how do price changes (SCD2 versions) affect conversion?
-- Compare the 7 days before vs. the 7 days after each price change of a product.
with versions as (
    select
        product_id,
        category_l1,
        brand,
        list_price,
        valid_from,
        lag(list_price) over (partition by product_id order by valid_from) as prev_price
    from {{ ref('dim_product') }}
),

changes as (
    select
        product_id,
        category_l1,
        brand,
        prev_price,
        list_price                                   as new_price,
        valid_from                                   as changed_at,
        (list_price - prev_price) / nullif(prev_price, 0) as price_change_pct
    from versions
    where prev_price is not null and prev_price <> list_price
),

windowed as (
    select
        c.product_id,
        c.changed_at,
        count_if(e.event_time <  c.changed_at and e.event_type = 'view')     as views_before,
        count_if(e.event_time <  c.changed_at and e.event_type = 'purchase') as purchases_before,
        count_if(e.event_time >= c.changed_at and e.event_type = 'view')     as views_after,
        count_if(e.event_time >= c.changed_at and e.event_type = 'purchase') as purchases_after
    from changes as c
    join {{ ref('fact_events') }} as e
        on  e.product_id = c.product_id
        and e.event_time >= c.changed_at - interval '7' day
        and e.event_time <  c.changed_at + interval '7' day
    group by 1, 2
)

select
    c.*,
    w.views_before,
    w.purchases_before,
    w.views_after,
    w.purchases_after,
    cast(w.purchases_before as double) / nullif(w.views_before, 0) as conv_before,
    cast(w.purchases_after as double)  / nullif(w.views_after, 0)  as conv_after,
    case
        when c.price_change_pct <= -0.10 then 'discount >= 10%'
        when c.price_change_pct <   0    then 'discount < 10%'
        when c.price_change_pct <  0.10  then 'increase < 10%'
        else 'increase >= 10%'
    end as change_bucket
from changes as c
left join windowed as w on w.product_id = c.product_id and w.changed_at = c.changed_at
