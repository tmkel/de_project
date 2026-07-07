with national as (
    select
        period_start,
        period_end,
        period_date,
        0 as region_id,
        fuel_type,
        fuel_perc
    from {{ ref('stg_national_generation_mix') }}
),

regional as (
    select
        period_start,
        period_end,
        period_date,
        region_id,
        fuel_type,
        fuel_perc
    from {{ ref('stg_regional_intensity_generation_mix') }}
),

combined as (
    select * from national
    union all
    select * from regional
)

select
    {{ dbt_utils.generate_surrogate_key(['combined.period_start', 'combined.region_id', 'combined.fuel_type']) }} as generation_key,
    dim_date.date_key,
    dim_region.region_key,
    dim_fuel.fuel_key,
    combined.period_start,
    combined.period_end,
    combined.period_date,
    combined.region_id,
    combined.fuel_type,
    combined.fuel_perc
from combined
left join {{ ref('dim_date') }} dim_date
    on combined.period_date = dim_date.full_date
left join {{ ref('dim_region') }} dim_region
    on combined.region_id = dim_region.region_id
left join {{ ref('dim_fuel') }} dim_fuel
    on combined.fuel_type = dim_fuel.fuel_type
