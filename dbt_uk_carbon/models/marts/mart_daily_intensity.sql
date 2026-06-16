with half_hourly as (
    select * from {{ ref('fct_half_hourly_intensity') }}
),

daily_agg as (
    select
        {{ dbt_utils.generate_surrogate_key(['period_date', 'region_id']) }} as daily_intensity_key,
        date_key,
        region_key,
        period_date,
        region_id,
        count(*)                                                        as periods_count,
        avg(forecast_intensity)                                         as avg_forecast_intensity,
        avg(actual_intensity)                                           as avg_actual_intensity,
        min(actual_intensity)                                           as min_actual_intensity,
        max(actual_intensity)                                           as max_actual_intensity,
        round(count(actual_intensity)::numeric / 48 * 100, 1)          as actual_completeness_pct,
        mode() within group (order by intensity_index)                  as dominant_index
    from half_hourly
    group by period_date, region_id, date_key, region_key
)

select * from daily_agg
order by period_date, region_id