with national as (
    select
        period_start,
        period_end,
        period_date,
        0 as region_id,
        intensity_forecast,
        intensity_actual,
        intensity_index
    from {{ ref('stg_national_intensity') }}
),

regional_deduped as (
    -- raw_regional has one row per (period, region, fuel)
    -- intensity is the same across fuels for a given period+region
    -- so we deduplicate by picking one fuel row per period+region
    select distinct on (period_start, region_id)
        period_start,
        period_end,
        period_date,
        region_id,
        intensity_forecast,
        null::numeric as intensity_actual,
        intensity_index
    from {{ ref('stg_regional_intensity_generation_mix') }}
    order by period_start, region_id, fuel_type
),

combined as (
    select * from national
    union all
    select * from regional_deduped
)

select
    {{ dbt_utils.generate_surrogate_key(['combined.period_start', 'combined.region_id']) }} as intensity_key,
    dim_date.date_key,
    dim_region.region_key,
    combined.period_start,
    combined.period_end,
    combined.period_date,
    combined.region_id,
    combined.intensity_forecast,
    combined.intensity_actual,
    combined.intensity_index
from combined
left join {{ ref('dim_date') }} dim_date
    on combined.period_date = dim_date.full_date
left join {{ ref('dim_region') }} dim_region
    on combined.region_id = dim_region.region_id
