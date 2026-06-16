"""
Glue PySpark job: re-implement fct_daily_intensity from raw staging parquet.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session

raw_path = f"s3://{bucket}/raw/national_intensity/"
out_path = f"s3://{bucket}/curated/mart_daily_intensity/"

df = spark.read.parquet(raw_path)

# rename dotted and reserved columns before any transforms
df = (
    df.withColumnRenamed("from", "period_from")
      .withColumnRenamed("to", "period_to")
      .withColumnRenamed("intensity.forecast", "forecast_intensity")
      .withColumnRenamed("intensity.actual", "actual_intensity")
      .withColumnRenamed("intensity.index", "intensity_index")
)

# extract date from the period_from timestamp string e.g. "2025-01-01T00:00Z"
df = df.withColumn(
    "period_date",
    F.to_date(F.col("period_from"), "yyyy-MM-dd'T'HH:mm'Z'")
)

# aggregate half-hourly rows to daily — national grain, region_id = 0
daily = (
    df.withColumn("region_id", F.lit(0))
      .groupBy("period_date", "region_id")
      .agg(
          F.count("*").alias("periods_count"),
          F.avg("forecast_intensity").alias("avg_forecast_intensity"),
          F.avg("actual_intensity").alias("avg_actual_intensity"),
          F.min("actual_intensity").alias("min_actual_intensity"),
          F.max("actual_intensity").alias("max_actual_intensity"),
          F.round(
              F.count("actual_intensity") / F.lit(48) * 100, 1
          ).alias("actual_completeness_pct"),
      )
      .withColumn(
          "sk_daily_intensity",
          F.sha2(
              F.concat_ws("||",
                  F.col("period_date").cast("string"),
                  F.col("region_id").cast("string")
              ), 256
          ),
      )
)

(daily.write
      .mode("overwrite")
      .partitionBy("period_date")
      .parquet(out_path))

print(f"wrote {daily.count()} rows to {out_path}")