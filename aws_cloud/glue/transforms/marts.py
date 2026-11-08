from pyspark.sql import functions as F

from .common import dbt_key


def build_mart_daily_intensity(fct_half_hourly_intensity_df):
    """Build mart_daily_intensity from fct_half_hourly_intensity."""
    daily = fct_half_hourly_intensity_df.groupBy(
        "period_date", "region_id", "date_key", "region_key"
    ).agg(
        F.count("*").alias("periods_count"),
        F.avg("intensity_forecast").alias("avg_intensity_forecast"),
        F.avg("intensity_actual").alias("avg_intensity_actual"),
        F.min("intensity_actual").alias("min_intensity_actual"),
        F.max("intensity_actual").alias("max_intensity_actual"),
        F.round(F.count("intensity_actual") / 48 * 100, 1).alias(
            "intensity_actual_completeness_pct"
        ),
    )

    # same column order as dbt's mart_daily_intensity (minus dominant_index)
    return daily.select(
        dbt_key("period_date", "region_id").alias("daily_intensity_key"),
        "date_key",
        "region_key",
        "period_date",
        "region_id",
        "periods_count",
        "avg_intensity_forecast",
        "avg_intensity_actual",
        "min_intensity_actual",
        "max_intensity_actual",
        "intensity_actual_completeness_pct",
    )
