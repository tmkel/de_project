with source as (
    select * from {{ source('raw', 'raw_national_intensity') }}
),

cleaned as (
    select
        from_date::timestamp            as period_from,
        to_date::timestamp              as period_to,
        forecast                        as forecast_intensity,
        actual                          as actual_intensity,
        intensity_index,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned