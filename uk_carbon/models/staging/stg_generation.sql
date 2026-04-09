with source as (
    select * from {{ source('raw', 'raw_generation') }}
),

cleaned as (
    select
        from_date::timestamp            as period_from,
        to_date::timestamp              as period_to,
        lower(trim(fuel))               as fuel_type,
        perc                            as fuel_percentage,
        (from_date::timestamp)::date    as period_date,
        loaded_at
    from source
)

select * from cleaned