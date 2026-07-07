-- Intensity values should be 0-1000 gCO2/kWh
-- Any rows returned = test failure

select
    period_start,
    intensity_forecast,
    intensity_actual
from {{ ref('stg_national_intensity') }}
where intensity_forecast < 0 or intensity_forecast > 1000
   or (intensity_actual is not null and (intensity_actual < 0 or intensity_actual > 1000))