"""
Pipeline Step: Glue PySpark job
- This reproduces the mart_daily_intensity dbt lineage end to end on the S3 raw (staged) parquet:
    stg_national_intensity + stg_regional
        -> fct_half_hourly_intensity  (national region_id=0 + deduped regional)
        -> mart_daily_intensity        (daily aggregates per region)

Ref: Overall data flow

                     UK Carbon Intensity API
                            |
                     Ingestion (Python + requests)
                            |
                     Validation (Pydantic)
                            |
                     Staging (JSON -> Parquet)
                      /                    \
        Postgres raw load            S3 raw  (s3_loader)
              |                              |
    dbt  (stg -> core -> marts)      Glue PySpark jobs
              |                              |
      Postgres marts                 S3 curated  ->  (Iceberg -> Athena - planned)
"""


import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session

national_path = f"s3://{bucket}/raw/national_intensity/"
regional_path = f"s3://{bucket}/raw/regional_intensity/"
out_path = f"s3://{bucket}/curated/mart_daily_intensity/"

# the API timestamp string is e.g. "2025-01-01T00:00Z"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'"

def make_key(*cols):
    """Readable composite key for a row's grain."""
    return F.concat_ws("|", *[F.col(c).cast("string") for c in cols])

# ---------------------------------------------------------------------------
# stg_national_intensity side of fct_half_hourly_intensity
# national grid is modelled as region_id = 0 and carries actual_intensity
# ---------------------------------------------------------------------------
national_df = spark.read.parquet(national_path)

national = (
    national_df
    # rename dotted/reserved staged columns (dots are literal name chars here)
    .withColumnRenamed("from", "period_from")
    .withColumnRenamed("to", "period_to")
    .withColumnRenamed("intensity.forecast", "forecast_intensity")
    .withColumnRenamed("intensity.actual", "actual_intensity")
    .withColumnRenamed("intensity.index", "intensity_index")
    .withColumn("region_id", F.lit(0).cast("int"))
    .withColumn("period_date", F.to_date(F.col("period_from"), TS_FORMAT))
    .select(
        "period_date",
        "period_from",
        "period_to",
        "region_id",
        F.col("forecast_intensity").cast("double").alias("forecast_intensity"),
        F.col("actual_intensity").cast("double").alias("actual_intensity"),
        "intensity_index",
    )
)


# ---------------------------------------------------------------------------
# stg_regional side of fct_half_hourly_intensity
# input has one row per (period, region, fuel), intensity is identical across fuels, so dedup to one row per (period_from, region_id)
# regional has no actual_intensity
# ---------------------------------------------------------------------------
regional_df = spark.read.parquet(regional_path)

regional_cleaned = (
    regional_df
    .withColumnRenamed("from", "period_from")
    .withColumnRenamed("to", "period_to")
    .withColumnRenamed("regionid", "region_id")
    .withColumnRenamed("intensity.forecast", "forecast_intensity")
    .withColumnRenamed("intensity.index", "intensity_index")
    .withColumn("region_id", F.col("region_id").cast("int"))
    .withColumn("period_date", F.to_date(F.col("period_from"), TS_FORMAT))
    .withColumn("fuel_type", F.lower(F.trim(F.col("fuel"))))
)

dedup_window = Window.partitionBy("period_from", "region_id").orderBy(F.asc("fuel_type"))

regional = (
    regional_cleaned
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .select(
        "period_date",
        "period_from",
        "period_to",
        "region_id",
        F.col("forecast_intensity").cast("double").alias("forecast_intensity"),
        F.lit(None).cast("double").alias("actual_intensity"),
        "intensity_index",
    )
)

# ---------------------------------------------------------------------------
# combine national and regional ones to match fct_half_hourly_intensity
# ---------------------------------------------------------------------------
combined = (
    national.unionByName(regional)
    .withColumn("date_key", make_key("period_date"))
    .withColumn("region_key", make_key("region_id"))
)



# ---------------------------------------------------------------------------
# daily aggregation (mart_daily_intensity)
# ---------------------------------------------------------------------------
daily = (
    combined
    .groupBy("period_date", "region_id", "date_key", "region_key")
    .agg(
        F.count("*").alias("periods_count"),
        F.avg("forecast_intensity").alias("avg_forecast_intensity"),
        F.avg("actual_intensity").alias("avg_actual_intensity"),
        F.min("actual_intensity").alias("min_actual_intensity"),
        F.max("actual_intensity").alias("max_actual_intensity"),
        F.round(F.count("actual_intensity") / F.lit(48) * 100, 1).alias(
            "actual_completeness_pct"
        ),
    )
    .withColumn("daily_intensity_key", make_key("period_date", "region_id"))
)


# select and order columns to match the dbt mart
daily = daily.select(
    "daily_intensity_key",
    "date_key",
    "region_key",
    "period_date",
    "region_id",
    "periods_count",
    "avg_forecast_intensity",
    "avg_actual_intensity",
    "min_actual_intensity",
    "max_actual_intensity",
    "actual_completeness_pct",
).orderBy("period_date", "region_id")

(daily.write
      .mode("overwrite")
      .partitionBy("period_date")
      .parquet(out_path))

print(f"wrote {daily.count()} rows to {out_path}")
