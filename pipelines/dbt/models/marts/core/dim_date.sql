with bounds as (
    select
        coalesce(min(event_date), current_date) as d0,
        greatest(coalesce(max(event_date), current_date), current_date) as d1
    from {{ source('silver', 'events') }}
),

days as (
    select d as date_day
    from bounds
    cross join unnest(sequence(d0, d1, interval '1' day)) as t(d)
)

select
    cast(date_format(days.date_day, '%Y%m%d') as integer) as date_key,
    days.date_day,
    year(days.date_day)                                   as year,
    quarter(days.date_day)                                as quarter,
    month(days.date_day)                                  as month,
    date_format(days.date_day, '%Y-%m')                   as year_month,
    week_of_year(days.date_day)                           as iso_week,
    day_of_week(days.date_day)                            as iso_day_of_week,
    date_format(days.date_day, '%W')                      as day_name,
    day_of_week(days.date_day) in (6, 7)                  as is_weekend,
    h.holiday_name,
    h.holiday_type,
    h.holiday_date is not null                            as is_holiday
from days
left join {{ ref('holidays') }} as h
    on h.holiday_date = days.date_day
