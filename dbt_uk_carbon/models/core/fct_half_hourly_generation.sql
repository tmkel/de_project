with national as (
    select
        period_from,
        period_to,
        period_date,
        0 as region_id,
        fuel_type,
        fuel_percentage
    from {{ ref('stg_generation') }}
),

regional as (
    select
        period_from,
        period_to,
        period_date,
        region_id,
        fuel_type,
        fuel_percentage
    from {{ ref('stg_regional') }}
),

combined as (
    select * from national
    union all
    select * from regional
)

select
    {{ dbt_utils.generate_surrogate_key(['combined.period_from', 'combined.region_id', 'combined.fuel_type']) }} as generation_key,
    dim_date.date_key,
    dim_region.region_key,
    dim_fuel.fuel_key,
    combined.period_from,
    combined.period_to,
    combined.period_date,
    combined.region_id,
    combined.fuel_type,
    combined.fuel_percentage
from combined
left join {{ ref('dim_date') }} dim_date
    on combined.period_date = dim_date.full_date
left join {{ ref('dim_region') }} dim_region
    on combined.region_id = dim_region.region_id
left join {{ ref('dim_fuel') }} dim_fuel
    on combined.fuel_type = dim_fuel.fuel_type
