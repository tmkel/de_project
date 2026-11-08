from pyspark.sql import functions as F

TS_FORMAT = "yyyy-MM-dd'T'HH:mmX"


def stg_national_intensity(df):
    return df.select(
        F.to_timestamp(F.col("from"), TS_FORMAT).alias("period_start"),
        F.to_timestamp(F.col("to"), TS_FORMAT).alias("period_end"),
        F.col("`intensity.forecast`").cast("int").alias("intensity_forecast"),
        F.col("`intensity.actual`").cast("int").alias("intensity_actual"),
        F.col("`intensity.index`").cast("string").alias("intensity_index"),
        F.to_date(F.col("from"), TS_FORMAT).alias("period_date"),
    )


def stg_national_generation_mix(df):
    return df.select(
        F.to_timestamp(F.col("from"), TS_FORMAT).alias("period_start"),
        F.to_timestamp(F.col("to"), TS_FORMAT).alias("period_end"),
        F.trim(F.lower(F.col("fuel").cast("string"))).alias("fuel_type"),
        F.col("perc").cast("double").alias("fuel_perc"),
        F.to_date(F.col("from"), TS_FORMAT).alias("period_date"),
    )


def stg_regional_intensity_generation_mix(df):
    return df.select(
        F.to_timestamp(F.col("from"), TS_FORMAT).alias("period_start"),
        F.to_timestamp(F.col("to"), TS_FORMAT).alias("period_end"),
        F.col("regionid").cast("int").alias("region_id"),
        F.col("dnoregion").cast("string").alias("dno_region"),
        F.col("shortname").cast("string").alias("region_shortname"),
        F.col("`intensity.forecast`").cast("int").alias("intensity_forecast"),
        F.col("`intensity.index`").cast("string").alias("intensity_index"),
        F.trim(F.lower(F.col("fuel").cast("string"))).alias("fuel_type"),
        F.col("perc").cast("double").alias("fuel_perc"),
        F.to_date(F.col("from"), TS_FORMAT).alias("period_date"),
    )


def dbt_key(*cols):
    """Byte-for-byte replica of dbt_utils.generate_surrogate_key:
    md5 of the null-safe string casts, joined with '-'.

    Each col may be a column name or a Column expression."""
    return F.md5(
        F.concat_ws(
            "-",
            *[
                F.coalesce(
                    (F.col(c) if isinstance(c, str) else c).cast("string"),
                    F.lit("_dbt_utils_surrogate_key_null_"),
                )
                for c in cols
            ],
        )
    )
