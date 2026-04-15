# uk_carbon dbt project

dbt transformation layer for the UK Carbon Intensity pipeline.

## Models

**Staging** (views): `stg_national_intensity`, `stg_generation`, `stg_regional`

**Core** (tables): `dim_date`, `dim_fuel`, `dim_region`, `fct_half_hourly_intensity`, `fct_half_hourly_generation`

**Marts** (tables): `mart_daily_intensity_by_region`, `mart_daily_generation_mix`, `mart_forecast_accuracy`

## Usage

```bash
# From the uk_carbon/ directory
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
```

## Tests

54 data tests including referential integrity checks (relationships), uniqueness, not-null, accepted values, and a custom range test for intensity values (0-1000 gCO2/kWh).
