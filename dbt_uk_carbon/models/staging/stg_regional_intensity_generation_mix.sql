with source as (
    select * from {{ source('raw', 'raw_regional_intensity_generation_mix') }}
),

cleaned as (
    select
        from_date::timestamp            as period_start,
        to_date::timestamp              as period_end,
        regionid                        as region_id,
        dnoregion                       as dno_region,
        shortname                       as region_shortname,
        intensity_forecast              as intensity_forecast,
        intensity_index                 as intensity_index,
        lower(trim(fuel))               as fuel_type,
        perc                            as fuel_perc,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned