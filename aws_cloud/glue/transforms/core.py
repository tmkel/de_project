from pyspark.sql import Window
from pyspark.sql import functions as F

from .common import dbt_key


def build_fct_half_hourly_intensity(
    stg_national_intensity_df, stg_regional_intensity_generation_mix_df
):
    """Build fct_half_hourly_intensity from stg_national_intensity and stg_regional_intensity_generation_mix."""

    FCT_COLUMNS = [
        "period_start",
        "period_end",
        "period_date",
        "region_id",
        "intensity_forecast",
        "intensity_actual",
        "intensity_index",
    ]

    national = stg_national_intensity_df.withColumn("region_id", F.lit(0).cast("int"))

    w = Window.partitionBy("period_start", "region_id").orderBy("fuel_type")
    regional = (
        stg_regional_intensity_generation_mix_df.withColumn(
            "intensity_actual", F.lit(None).cast("int")
        )
        .withColumn("row_number", F.row_number().over(w))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    combined = national.select(FCT_COLUMNS).unionByName(regional.select(FCT_COLUMNS))

    return combined.select(
        dbt_key("period_start", "region_id").alias("intensity_key"),
        dbt_key(F.col("period_date").cast("timestamp")).alias("date_key"),
        dbt_key("region_id").alias("region_key"),
        *FCT_COLUMNS,
    )
