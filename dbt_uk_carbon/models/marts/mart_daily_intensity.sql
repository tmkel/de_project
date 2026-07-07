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
        avg(intensity_forecast)                                         as avg_intensity_forecast,
        avg(intensity_actual)                                           as avg_intensity_actual,
        min(intensity_actual)                                           as min_intensity_actual,
        max(intensity_actual)                                           as max_intensity_actual,
        round(count(intensity_actual)::numeric / 48 * 100, 1)           as intensity_actual_completeness_pct,
        mode() within group (order by intensity_index)                  as dominant_index
    from half_hourly
    group by period_date, region_id, date_key, region_key
)

select * from daily_agg
order by period_date, region_id