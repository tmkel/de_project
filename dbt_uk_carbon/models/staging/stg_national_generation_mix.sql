with source as (
    select * from {{ source('raw', 'raw_national_generation_mix') }}
),

cleaned as (
    select
        from_date::timestamp            as period_start,
        to_date::timestamp              as period_end,
        lower(trim(fuel))               as fuel_type,
        perc                            as fuel_perc,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned