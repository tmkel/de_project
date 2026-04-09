with half_hourly as (
    select * from {{ ref('stg_generation') }}
),

daily_fuel as (
    select
        period_date,
        fuel_type,
        avg(fuel_percentage)    as avg_percentage,
        min(fuel_percentage)    as min_percentage,
        max(fuel_percentage)    as max_percentage,
        count(*)                as periods_count
    from half_hourly
    group by period_date, fuel_type
),

with_category as (
    select
        df.*,
        dim.fuel_category
    from daily_fuel df
    left join {{ ref('dim_fuel') }} dim
        on df.fuel_type = dim.fuel_type
)

select * from with_category
order by period_date, fuel_type