# UK Carbon Intensity Data Pipeline

A production-style data pipeline that ingests, validates, transforms, and serves UK electricity grid carbon intensity data. Built with Python, dbt, Apache Airflow, PostgreSQL, and Docker.

## What It Does

This pipeline pulls half-hourly carbon intensity and generation mix data from the [UK Carbon Intensity API](https://carbonintensity.org.uk/), validates it with Pydantic, stages it as Parquet, loads it into PostgreSQL, and transforms it through a dimensional model using dbt. Airflow orchestrates the full workflow on a daily schedule.

The data covers three dimensions of the UK grid: national carbon intensity (forecast vs. actual), generation mix by fuel type, and regional breakdowns across 19 UK grid regions (14 DNO regions, National Grid, and country-level aggregates).

## Architecture

```
UK Carbon Intensity API
        |
        v
 +-----------------+
 |   Ingestion     |  Python + requests
 |   (api_client)  |  3 endpoints, rate-limited
 +-----------------+
        |
        v
 +-----------------+
 |   Validation    |  Pydantic models
 |   (schemas)     |  Type + range checks
 +-----------------+
        |
        v
 +-----------------+
 |   Staging       |  JSON -> Parquet
 |   (staging.py)  |  Flatten nested structures
 +-----------------+
        |
        v
 +-----------------+
 |   Raw Load      |  Idempotent delete-then-insert
 |   (raw_loader)  |  PostgreSQL raw tables
 +-----------------+
        |
        v
 +-----------------+
 |   Transform     |  dbt (staging -> core -> marts)
 |   (uk_carbon)   |  Dimensional model, 54 tests
 +-----------------+
        |
        v
 +-----------------+
 |   Orchestration |  Airflow DAGs
 |   (3 DAGs)      |  Daily schedule + backfill
 +-----------------+

 All services containerized with Docker Compose
```

## Data Model

The dbt project follows a staging/core/marts pattern:

**Staging layer** (views): Light cleaning of raw tables — type casting, column renaming, null handling.
- `stg_national_intensity`, `stg_generation`, `stg_regional`

**Core layer** (tables): Dimensional model with surrogate keys.
- `dim_date` — Date spine (2022-2027) with day/week/month attributes
- `dim_fuel` — Fuel types categorized as renewable, fossil, or other
- `dim_region` — 19 UK grid regions (14 DNO regions, National Grid, and country-level aggregates)
- `fct_half_hourly_intensity` — Grain: period x region (forecast and actual intensity)
- `fct_half_hourly_generation` — Grain: period x region x fuel (generation percentages)

Two separate fact tables because the grains differ: intensity is one row per period per region, while generation is one row per period per region per fuel type.

**Marts layer** (tables): Business-level aggregations.
- `mart_daily_intensity_by_region` — Daily intensity summaries with completeness tracking
- `mart_daily_generation_mix` — Daily fuel mix with median and average percentages
- `mart_forecast_accuracy` — Forecast vs. actual error metrics with accuracy bands

## Airflow DAGs

| DAG | Schedule | Purpose |
|---|---|---|
| `ingest_and_load` | Daily at 02:00 UTC | Fetch yesterday's data, validate, stage, load to PostgreSQL |
| `transform` | Daily at 03:00 UTC | Run `dbt build` (models + tests) after data freshness check |
| `backfill_historical` | Manual trigger | Load historical data in configurable date ranges |

The transform DAG checks data freshness before running dbt, rather than using ExternalTaskSensor. This is more robust because it decouples the DAGs from Airflow's scheduling metadata.

## Tech Stack

| Component | Tool | Why |
|---|---|---|
| Language | Python 3.12 | Primary language for data engineering |
| Validation | Pydantic | Schema enforcement before data touches storage |
| Staging | Pandas + Parquet | Efficient columnar format for staging |
| Database | PostgreSQL 16 | Reliable serving layer with SQL access |
| Transforms | dbt | Industry standard — testable, documented, version-controlled SQL |
| Orchestration | Apache Airflow 2.10 | DAG-based scheduling with retry policies |
| Containers | Docker Compose | Reproducible multi-service setup |
| Package mgmt | uv | Fast Python dependency management |

## Setup

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ with [uv](https://docs.astral.sh/uv/)

### Run with Docker (recommended)

```bash
# Clone the repo
git clone https://github.com/tmkel/de_project.git
cd de_project

# Create .env from template
cp .env.example .env

# Start all services (PostgreSQL + Airflow)
docker compose up -d

# Airflow UI available at http://localhost:8080
# Default login: admin / admin
```

### Run locally (without Airflow)

```bash
# Install dependencies
uv sync

# Start PostgreSQL
docker compose up postgres -d

# Create .env with your database credentials
cp .env.example .env

# Run the pipeline for a date range
uv run python run_pipeline.py --from-date 2024-01-01 --to-date 2024-01-07

# Run dbt (models + tests)
cd uk_carbon
dbt build --profiles-dir .
```

## Project Structure

```
de_project/
├── dags/                        # Airflow DAG definitions
│   ├── ingest_and_load.py       #   Daily ingestion pipeline
│   ├── transform.py             #   dbt build with freshness check
│   └── backfill.py              #   Historical data loader
├── src/
│   ├── ingestion/
│   │   └── api_client.py        # UK Carbon Intensity API client
│   ├── models/
│   │   └── schemas.py           # Pydantic validation models
│   └── storage/
│       ├── init_db.sql          # PostgreSQL schema DDL
│       ├── raw_loader.py        # Idempotent loader (delete-then-insert)
│       └── staging.py           # JSON -> Parquet staging
├── uk_carbon/                   # dbt project
│   └── models/
│       ├── staging/             #   3 staging views
│       ├── core/                #   3 dims + 2 facts
│       └── marts/               #   3 aggregated tables
├── tests/                       # Python test suite
├── docker-compose.yml           # PostgreSQL + Airflow services
├── Dockerfile                   # Pipeline image
├── Dockerfile.airflow           # Airflow image
├── run_pipeline.py              # CLI entry point
└── .env.example                 # Environment variable template
```

## Design Decisions

**Why ELT, not ETL?** Raw data lands in PostgreSQL first, then dbt transforms it in place. This preserves the raw data for reprocessing and lets the database engine handle transformations efficiently.

**Why two fact tables?** The intensity and generation data have different grains. Forcing them into one wide table would either duplicate rows or lose granularity. Two atomic fact tables joined through shared dimensions is the dimensional modeling best practice.

**Why batch, not streaming?** The source API updates every 30 minutes. Streaming infrastructure (Kafka, Flink) would add complexity with no architectural benefit at this update frequency.

**Why shared PostgreSQL for pipeline data and Airflow metadata?** Simplicity. For a single-machine deployment, running two PostgreSQL instances adds operational overhead without meaningful isolation benefits.

**Why LocalExecutor?** The pipeline runs on a single machine. CeleryExecutor with Redis would add infrastructure cost for no concurrency gain at this scale.

## Future Enhancements

- **S3 data lake**: Move raw/staging storage from local files to S3 with a storage abstraction layer, keeping local as a dev fallback.
- **CI/CD**: GitHub Actions pipeline running pytest, ruff, and `dbt compile` on every PR.
- **Second data source**: Add Climate TRACE bulk CSV as a file-based ingestion pattern, with a unified staging layer mapping both sources to a common schema.
- **Data quality framework**: Completeness checks (no missing half-hour slots), freshness monitoring, and outlier detection with results logged to a quality table.
