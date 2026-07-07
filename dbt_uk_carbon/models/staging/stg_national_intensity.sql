with source as (
    select * from {{ source('raw', 'raw_national_intensity') }}
),

cleaned as (
    select
        from_date::timestamp            as period_start,
        to_date::timestamp              as period_end,
        intensity_forecast              as intensity_forecast,
        intensity_actual                as intensity_actual,
        intensity_index                 as intensity_index,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned