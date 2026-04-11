with half_hourly as (
    select * from {{ ref('fct_half_hourly_generation') }}
),

daily_fuel as (
    select
        {{ dbt_utils.generate_surrogate_key(['period_date', 'region_id', 'fuel_type']) }} as daily_generation_key,
        date_key,
        region_key,
        fuel_key,
        period_date,
        region_id,
        fuel_type,
        -- Note: avg_percentage is an unweighted average across half-hourly periods.
        -- A true daily percentage would require weighting by total generation volume (GW),
        -- which is not available from the Carbon Intensity API.
        -- This approximation treats all half-hourly periods as equal regardless of demand.
        avg(fuel_percentage)                    as avg_percentage,
        percentile_cont(0.5) within group (order by fuel_percentage)    as median_percentage,
        min(fuel_percentage)                    as min_percentage,
        max(fuel_percentage)                    as max_percentage,
        count(*)                                as periods_count
    from half_hourly
    group by period_date, region_id, fuel_type, date_key, region_key, fuel_key
)

select
    df.*,
    dim.fuel_category
from daily_fuel df
left join {{ ref('dim_fuel') }} dim
    on df.fuel_type = dim.fuel_type
order by period_date, region_id, fuel_type