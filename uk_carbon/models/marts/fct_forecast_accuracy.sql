with half_hourly as (
    select * from {{ ref('stg_national_intensity') }}
    where actual_intensity is not null
),

with_error as (
    select
        period_from,
        period_to,
        period_date,
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
        period_date,
        count(*)                                                        as periods_with_actual,
        round(avg(absolute_error)::numeric, 2)                          as avg_error,
        round(avg(pct_error)::numeric, 2)                               as avg_pct_error,
        round(sum(case when accuracy_band = 'accurate' then 1 else 0 end)::numeric
            / count(*) * 100, 1)                                        as pct_accurate,
        round(sum(case when accuracy_band = 'off' then 1 else 0 end)::numeric
            / count(*) * 100, 1)                                        as pct_off
    from with_error
    group by period_date
)

select * from daily_accuracy
order by period_date