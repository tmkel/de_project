with fuels as (
    select distinct fuel_type
    from (
        select fuel_type from {{ ref('stg_generation') }}
        union
        select fuel_type from {{ ref('stg_regional') }}
    ) all_fuels
),

categorized as (
    select
        {{ dbt_utils.generate_surrogate_key(['fuel_type']) }} as fuel_key,
        fuel_type,
        case
            when fuel_type in ('wind', 'solar', 'hydro', 'biomass') then 'renewable'
            when fuel_type in ('gas', 'coal') then 'fossil'
            else 'other'
        end as fuel_category
    from fuels
)

select * from categorized
order by fuel_category, fuel_type