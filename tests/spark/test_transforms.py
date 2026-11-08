import datetime
import hashlib
from pathlib import Path

from pyspark.sql import functions as F

from transforms.common import (
    TS_FORMAT,
    dbt_key,
    stg_national_generation_mix,
    stg_national_intensity,
    stg_regional_intensity_generation_mix,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "data" / "staging"


def _md5(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def test_stg_national_intensity_renames_and_parses(spark):
    raw = spark.createDataFrame(
        [("2026-01-01T23:30Z", "2026-01-02T00:00Z", 105, 98, "low")],
        ["from", "to", "intensity.forecast", "intensity.actual", "intensity.index"],
    )

    out = stg_national_intensity(raw)
    row = out.collect()[0]

    assert out.columns == [
        "period_start",
        "period_end",
        "intensity_forecast",
        "intensity_actual",
        "intensity_index",
        "period_date",
    ]
    assert row["period_date"] == datetime.date(2026, 1, 1)
    assert row["intensity_forecast"] == 105


def test_stg_national_intensity_on_fixture_has_48_rows(spark):
    raw = spark.read.parquet(
        str(FIXTURES / "national_intensity" / "2026-01-01.parquet")
    )
    out = stg_national_intensity(raw)
    assert out.count() == 48
    assert out.filter(F.col("period_start").isNull()).count() == 0


def test_stg_national_generation_mix_renames_and_parses(spark):
    raw = spark.createDataFrame(
        [(" Wind ", 56.3, "2026-01-01T23:30Z", "2026-01-02T00:00Z")],
        ["fuel", "perc", "from", "to"],
    )

    out = stg_national_generation_mix(raw)
    row = out.collect()[0]

    assert out.columns == [
        "period_start",
        "period_end",
        "fuel_type",
        "fuel_perc",
        "period_date",
    ]
    assert row["period_date"] == datetime.date(2026, 1, 1)
    assert row["fuel_type"] == "wind"
    assert row["fuel_perc"] == 56.3


def test_stg_national_generation_mix_on_fixture_has_432_rows(spark):
    raw = spark.read.parquet(
        str(FIXTURES / "national_generation_mix" / "2026-01-01.parquet")
    )
    out = stg_national_generation_mix(raw)
    # 48 half-hour periods x 9 fuels
    assert out.count() == 432
    assert out.filter(F.col("period_start").isNull()).count() == 0


def test_stg_regional_intensity_renames_and_parses(spark):
    raw = spark.createDataFrame(
        [
            (
                "2026-01-01T23:30Z",
                "2026-01-02T00:00Z",
                1,
                "Scottish Hydro Electric Power Distribution",
                "North Scotland",
                42,
                "very low",
                " Wind ",
                87.5,
            )
        ],
        [
            "from",
            "to",
            "regionid",
            "dnoregion",
            "shortname",
            "intensity.forecast",
            "intensity.index",
            "fuel",
            "perc",
        ],
    )

    out = stg_regional_intensity_generation_mix(raw)
    row = out.collect()[0]

    assert out.columns == [
        "period_start",
        "period_end",
        "region_id",
        "dno_region",
        "region_shortname",
        "intensity_forecast",
        "intensity_index",
        "fuel_type",
        "fuel_perc",
        "period_date",
    ]
    assert row["period_date"] == datetime.date(2026, 1, 1)
    assert row["region_id"] == 1
    assert row["region_shortname"] == "North Scotland"
    assert row["intensity_forecast"] == 42
    assert row["fuel_type"] == "wind"
    assert row["fuel_perc"] == 87.5


def test_stg_regional_intensity_on_fixture_has_7776_rows(spark):
    raw = spark.read.parquet(
        str(FIXTURES / "regional_intensity_generation_mix" / "2026-01-01.parquet")
    )
    out = stg_regional_intensity_generation_mix(raw)
    # 48 half-hour periods x 18 regions x 9 fuels
    assert out.count() == 7776
    assert out.filter(F.col("period_start").isNull()).count() == 0


def test_dbt_key_single_column_is_md5_of_string(spark):
    # dim_region: generate_surrogate_key(['region_id'])
    df = spark.createDataFrame([(1,)], ["region_id"])
    key = df.select(dbt_key("region_id").alias("key")).collect()[0]["key"]
    assert key == _md5("1")


def test_dbt_key_timestamp_and_int_match_postgres_text_cast(spark):
    # fct_half_hourly_intensity: generate_surrogate_key(['period_start', 'region_id'])
    # Postgres '2026-01-01 00:30:00'::timestamp::text -> '2026-01-01 00:30:00'
    df = spark.createDataFrame([("2026-01-01T00:30Z", 1)], ["from", "region_id"])
    df = df.select(
        F.to_timestamp(F.col("from"), TS_FORMAT).alias("period_start"), "region_id"
    )
    key = df.select(dbt_key("period_start", "region_id").alias("key")).collect()[0][
        "key"
    ]
    assert key == _md5("2026-01-01 00:30:00-1")


def test_dbt_key_date_and_int_match_postgres_text_cast(spark):
    # mart_daily_intensity: generate_surrogate_key(['period_date', 'region_id'])
    df = spark.createDataFrame([("2026-01-01T00:30Z", 1)], ["from", "region_id"])
    df = df.select(
        F.to_date(F.col("from"), TS_FORMAT).alias("period_date"), "region_id"
    )
    key = df.select(dbt_key("period_date", "region_id").alias("key")).collect()[0][
        "key"
    ]
    assert key == _md5("2026-01-01-1")


def test_dbt_key_replaces_nulls_with_dbt_sentinel(spark):
    # without the coalesce, concat_ws would drop the null and (null, 1) would
    # hash the same as (1, null) and the same as just (1)
    df = spark.createDataFrame([(None, 1), (1, None)], "region_id int, fuel_type int")
    keys = [
        r["key"]
        for r in df.select(dbt_key("region_id", "fuel_type").alias("key")).collect()
    ]
    assert sorted(keys) == sorted(
        [
            _md5("_dbt_utils_surrogate_key_null_-1"),
            _md5("1-_dbt_utils_surrogate_key_null_"),
        ]
    )
