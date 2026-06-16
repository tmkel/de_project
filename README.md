# UK Carbon Intensity Data Pipeline

A production-style data pipeline that ingests, validates, transforms, and serves UK electricity grid carbon intensity data. Built with Python, dbt, Apache Airflow, PostgreSQL, and Docker — with a parallel AWS lakehouse layer (S3 + Glue/PySpark) that re-implements the transform tier on managed Spark.

## What It Does

This pipeline pulls half-hourly carbon intensity and generation mix data from the [UK Carbon Intensity API](https://carbonintensity.org.uk/), validates it with Pydantic, stages it as Parquet, loads it into PostgreSQL, and transforms it through a dimensional model using dbt. Airflow orchestrates the full workflow on a daily schedule.

In parallel, the same staging Parquet is landed in S3 and transformed by AWS Glue (PySpark) jobs that re-implement the dbt marts — demonstrating both transformation paradigms.

The data covers two dimensions of the UK grid: carbon intensity (forecast vs. actual) and generation mix by fuel type, at both nation and regional level.

## Architecture

Two transformation paths share the same validated staging layer.

```
                     UK Carbon Intensity API
                            |
                     Ingestion (Python + requests)
                            |
                     Validation (Pydantic)
                            |
                     Staging (JSON → Parquet)
                      /                    \
        Postgres raw load            S3 raw  (s3_loader)
              |                              |
    dbt  (stg → core → marts)        Glue PySpark jobs
              |                              |
      Postgres marts                 S3 curated  →  (Iceberg → Athena · planned)
```

## Two implementations, verified equivalent

dbt is the **production** transform; the Glue/PySpark layer is a **parallel** re-implementation, not a replacement.

- **dbt (production):** runs the full staging → core → marts dimensional model in PostgreSQL, with ~54 tests.
- **Glue/PySpark (cloud):** re-implements the mart layer on managed Spark, reading from S3 `raw/` and writing partitioned Parquet to S3 `curated/`. Currently covers the daily-intensity mart (national grain; per-region parity in progress); additional marts queued.
- **Equivalence:** `aws_cloud/verify/verify_equivalence.py` compares the dbt mart against the Glue output on the same date window (row counts + key metrics), documenting the few known differences (e.g. surrogate-key hashing, completeness rounding).

**Why keep both?** Production stays on dbt because the data volume doesn't justify a Spark cluster — but the Glue layer proves the same logic runs distributed and exercises the AWS lakehouse skill set. Maintaining a verified Spark twin of each mart is the point of the exercise.

## Data Model

The dbt project follows a staging/core/marts pattern:

**Staging layer** (views): Light cleaning of raw tables — type casting, column renaming, null handling.
- `stg_national_intensity`, `stg_generation`, `stg_regional`

**Core layer** (tables): Dimensional model with surrogate keys.
- `dim_date` — Date spine (2022-2027) with day/week/month attributes
- `dim_fuel` — Fuel types categorized as renewable, fossil, or other
- `dim_region` — 19 UK grid regions (14 DNO regions, National Grid, and country-level aggregates)
- `fct_half_hourly_intensity` — Grain: period × region (forecast and actual intensity)
- `fct_half_hourly_generation` — Grain: period × region × fuel (generation percentages)

Two separate fact tables because the grains differ: intensity is one row per period per region, while generation is one row per period per region per fuel type.

**Marts layer** (tables): Business-level aggregations.
- `mart_daily_intensity_by_region` — Daily intensity summaries with completeness tracking
- `mart_daily_generation_mix` — Daily fuel mix with median and average percentages
- `mart_forecast_accuracy` — Forecast vs. actual error metrics with accuracy bands

Each mart has (or is queued to have) a PySpark twin under `aws_cloud/glue/`, named `<model>_job.py`.

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `ingest_and_load` | Daily at 02:00 UTC | Fetch yesterday's data, validate, stage, load to PostgreSQL |
| `transform` | Daily at 03:00 UTC | Run `dbt build` (models + tests) after a data freshness check |
| `backfill` | Manual trigger | Load historical data in configurable date ranges |

The transform DAG checks data freshness before running dbt, rather than using ExternalTaskSensor. This is more robust because it decouples the DAGs from Airflow's scheduling metadata.

## Tech Stack

| Component | Tool | Why |
|---|---|---|
| Language | Python 3.12 | Primary language for data engineering |
| Validation | Pydantic | Schema enforcement before data touches storage |
| Staging | Pandas + Parquet | Efficient columnar format for staging |
| Database | PostgreSQL 16 | Reliable serving layer with SQL access |
| Transforms | dbt | Industry standard — testable, documented, version-controlled SQL |
| Orchestration (local) | Apache Airflow 2.10 | DAG-based scheduling with retry policies |
| Containers | Docker Compose | Reproducible multi-service setup |
| Package mgmt | uv | Fast Python dependency management |
| Cloud ETL | PySpark on AWS Glue | Re-implements the dbt marts on managed Spark; proves the same logic runs distributed |
| Object storage | Amazon S3 | Raw + curated zones for the lakehouse |
| Orchestration (cloud) | GitHub Actions | Runs the AWS layer; no idle cost (vs. a managed Airflow cluster) |
| Table format | Apache Iceberg | *Planned* — convert curated output to Iceberg in the Glue Data Catalog |
| Serving (cloud) | Amazon Athena + dbt-athena | *Planned* — serverless SQL over Iceberg as a second dbt target |
| IaC | Terraform | *Planned* — codify the S3/Glue/Athena resources |

## Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ with [uv](https://docs.astral.sh/uv/)

### Run with Docker (recommended)

```bash
# Clone the repo
git clone https://github.com/tmkel/de_project.git
cd de_project

# Create .env from template, then append your host UID
cp .env.example .env
mkdir -p logs
echo "AIRFLOW_UID=$(id -u)" >> .env

# Start all services (PostgreSQL + Airflow)
docker compose up -d

# Airflow UI at http://localhost:8080  (default login: admin / admin)
```

To view dbt docs after models are built, start the optional docs service:

```bash
docker compose --profile docs up dbt-docs   # dbt docs at http://localhost:8081
```

### Run locally (without Airflow)

```bash
uv sync
docker compose up postgres -d
cp .env.example .env

# Run the pipeline for a date range
uv run python run_pipeline.py --from-date 2024-01-01 --to-date 2024-01-07

# Run dbt (models + tests)
cd dbt_uk_carbon
dbt build --profiles-dir .
```

### Run the AWS lakehouse layer

```bash
# Land staging Parquet in S3 raw
uv run python -m src.storage.s3_loader --dataset all

# Submit the Glue job (after the job is registered — see aws_cloud/glue/README.md)
aws glue start-job-run --job-name carbon-mart-daily-intensity

# Verify Glue output matches the dbt mart
uv run python aws_cloud/verify/verify_equivalence.py
```

## Project Structure

```
de_project/

├── airflow_dags/                # Airflow DAG definitions

│   ├── ingest_and_load.py       #   Daily ingestion pipeline

│   ├── transform.py             #   dbt build with freshness check

│   └── backfill.py              #   Historical data loader

├── src/

│   ├── ingestion/

│   │   └── ingest_api.py        # UK Carbon Intensity API client

│   ├── validation/

│   │   └── validate.py          # Pydantic validation models

│   ├── staging/

│   │   └── staging.py           # JSON → Parquet staging

│   └── storage/

│       ├── init_db.sql          # PostgreSQL schema DDL

│       ├── psql_loader.py       # Idempotent loader (delete-then-insert)

│       └── s3_loader.py         # Upload staging Parquet to S3 raw zone

├── dbt_uk_carbon/               # dbt project

│   └── models/

│       ├── staging/             #   3 staging views

│       ├── core/                #   3 dims + 2 facts

│       └── marts/               #   3 aggregated tables

├── aws_cloud/                   # AWS lakehouse layer

│   ├── glue/

│   │   └── mart_daily_intensity_job.py   # PySpark twin of mart_daily_intensity_by_region

│   ├── verify/

│   ├── iceberg/                 #   planned

│   ├── athena/                  #   planned

│   └── terraform/               #   planned

├── data/                        # raw / staging / curated (gitignored)

├── tests/                       # Python test suite

├── docker-compose.yml           # PostgreSQL + Airflow services

├── Dockerfile.airflow           # Airflow image (uv-built from pyproject.toml)

├── run_pipeline.py              # CLI entry point

└── .env.example                 # Environment variable template
```

## Design Decisions

**Why ELT, not ETL?** Raw data lands in PostgreSQL first, then dbt transforms it in place. This preserves the raw data for reprocessing and lets the database engine handle transformations efficiently.

**Why two fact tables?** The intensity and generation data have different grains. Forcing them into one wide table would either duplicate rows or lose granularity. Two atomic fact tables joined through shared dimensions is the dimensional modeling best practice.

**Why keep dbt as production and add Glue in parallel?** The data volume doesn't justify a Spark cluster, so dbt stays production. Glue re-implements the marts to demonstrate the cloud/Spark skill set and to prove — via the verify step — that both paths agree. Ripping out dbt would tell a weaker story than maintaining a verified twin.

**Why GitHub Actions for the AWS layer, not managed Airflow?** Managed Airflow (MWAA) has no scale-to-zero and costs hundreds per month. GitHub Actions runs the cloud jobs at effectively zero idle cost and keeps the whole layer inside the free/near-free tier.

**Why batch, not streaming?** The source API updates every 30 minutes. Streaming infrastructure (Kafka, Flink) would add complexity with no architectural benefit at this update frequency.

**Why LocalExecutor?** The pipeline runs on a single machine. CeleryExecutor with Redis would add infrastructure cost for no concurrency gain at this scale.

## Roadmap

- **Per-region parity** for the Glue daily-intensity job (match `mart_daily_intensity_by_region` 1:1), plus PySpark twins of the generation-mix and forecast-accuracy marts.
- **Iceberg** (Session 6): convert curated Glue output to Iceberg tables in the Glue Data Catalog.
- **Athena + dbt-athena** (Session 7): serverless SQL over Iceberg as a second dbt target.
- **Terraform** (Session 8): codify the S3/Glue/Athena resources as IaC.
- **Second data source**: Climate TRACE bulk CSV via a file-based ingestion pattern, unified with the existing staging layer.