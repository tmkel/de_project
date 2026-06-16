-- Intensity values should be 0-1000 gCO2/kWh
-- Any rows returned = test failure

select
    period_from,
    forecast_intensity,
    actual_intensity
from {{ ref('stg_national_intensity') }}
where forecast_intensity < 0 or forecast_intensity > 1000
   or (actual_intensity is not null and (actual_intensity < 0 or actual_intensity > 1000))