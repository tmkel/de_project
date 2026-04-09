with regions as (
    select distinct
        region_id,
        dno_region,
        region_shortname
    from {{ ref('stg_regional') }}

    union all

    select
        0 as region_id,
        'National Grid' as dno_region,
        'National Grid' as region_shortname
)

select
    region_id,
    dno_region,
    region_shortname
from regions
order by region_id
