# AWS Glue Jobs

PySpark jobs that re-implement the dbt model layer on AWS, as a parallel
cloud implementation of the local `dbt_uk_carbon` project. dbt stays the
production transform; these jobs prove the same logic runs on Spark and
produce output verified equivalent to dbt (see `../verify/`).

## Naming conventions

dbt model prefixes (in `dbt_uk_carbon/models/`):

- `stg_`  — staging views (light cleaning)
- `dim_`  — dimension tables
- `fct_`  — atomic fact tables
- `mart_` — business-level aggregations

**Mirror rule:** each Glue job re-implements exactly one dbt model and is
named `<model>_job.py`. So `mart_daily_intensity_job.py` is the Spark twin
of dbt's `mart_daily_intensity`. The name tells you its dbt
counterpart at a glance.

## Jobs

| Glue job | dbt counterpart | Status |
|---|---|---|
| `mart_daily_intensity_job.py` | `mart_daily_intensity` | national grain only, will add remaining |