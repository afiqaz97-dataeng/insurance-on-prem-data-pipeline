-- Standard static date dimension. Range covers all observed dates in the data
-- (2021-01-02 to 2046-08-06, policy end_dates run furthest out) with padding.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2048-01-01' as date)"
    ) }}
)
select
    cast(date_day as date)               as date_day,
    extract(year from date_day)          as year,
    extract(quarter from date_day)       as quarter,
    extract(month from date_day)         as month,
    strftime(date_day, '%B')             as month_name,
    extract(day from date_day)           as day_of_month,
    extract(dow from date_day)           as day_of_week,
    strftime(date_day, '%A')             as day_name,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine
