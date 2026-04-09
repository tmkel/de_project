with source as (
    select * from {{ source('raw', 'raw_regional') }}
),

cleaned as (
    select
        from_date::timestamp            as period_from,
        to_date::timestamp              as period_to,
        regionid                        as region_id,
        dnoregion                       as dno_region,
        shortname                       as region_shortname,
        forecast                        as forecast_intensity,
        intensity_index,
        lower(trim(fuel))               as fuel_type,
        perc                            as fuel_percentage,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned