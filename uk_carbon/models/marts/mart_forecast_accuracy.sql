with half_hourly as (
    select * from {{ ref('fct_half_hourly_intensity') }}
    where actual_intensity is not null
),

with_error as (
    select
        date_key,
        region_key,
        period_from,
        period_to,
        period_date,
        region_id,
        forecast_intensity,
        actual_intensity,
        intensity_index,
        actual_intensity - forecast_intensity                           as absolute_error,
        round(
            abs(actual_intensity - forecast_intensity)::numeric
            / nullif(actual_intensity, 0) * 100, 2
        )                                                               as pct_error,
        case
            when abs(actual_intensity - forecast_intensity) <= 10 then 'accurate'
            when abs(actual_intensity - forecast_intensity) <= 30 then 'close'
            else 'off'
        end                                                             as accuracy_band
    from half_hourly
),

daily_accuracy as (
    select
        {{ dbt_utils.generate_surrogate_key(['period_date', 'region_id']) }} as forecast_accuracy_key,
        date_key,
        region_key,
        period_date,
        region_id,
        count(*)                                                        as periods_with_actual,
        round(avg(absolute_error)::numeric, 2)                          as avg_error,
        round(avg(pct_error)::numeric, 2)                               as avg_pct_error,
        round(sum(case when accuracy_band = 'accurate' then 1 else 0 end)::numeric
            / count(*) * 100, 1)                                        as pct_accurate,
        round(sum(case when accuracy_band = 'off' then 1 else 0 end)::numeric
            / count(*) * 100, 1)                                        as pct_off
    from with_error
    group by period_date, region_id, date_key, region_key
)

select * from daily_accuracy
order by period_date, region_id