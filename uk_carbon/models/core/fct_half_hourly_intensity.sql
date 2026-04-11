with national as (
    select
        period_from,
        period_to,
        period_date,
        0 as region_id,
        forecast_intensity,
        actual_intensity,
        intensity_index
    from {{ ref('stg_national_intensity') }}
),

regional_deduped as (
    -- raw_regional has one row per (period, region, fuel)
    -- intensity is the same across fuels for a given period+region
    -- so we deduplicate by picking one fuel row per period+region
    select distinct on (period_from, region_id)
        period_from,
        period_to,
        period_date,
        region_id,
        forecast_intensity,
        null::numeric as actual_intensity,
        intensity_index
    from {{ ref('stg_regional') }}
    order by period_from, region_id, fuel_type
),

combined as (
    select * from national
    union all
    select * from regional_deduped
)

select
    {{ dbt_utils.generate_surrogate_key(['combined.period_from', 'combined.region_id']) }} as intensity_key,
    dim_date.date_key,
    dim_region.region_key,
    combined.period_from,
    combined.period_to,
    combined.period_date,
    combined.region_id,
    combined.forecast_intensity,
    combined.actual_intensity,
    combined.intensity_index
from combined
left join {{ ref('dim_date') }} dim_date
    on combined.period_date = dim_date.full_date
left join {{ ref('dim_region') }} dim_region
    on combined.region_id = dim_region.region_id
