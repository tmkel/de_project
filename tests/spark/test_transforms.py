import datetime
import hashlib
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from transforms.common import (
    TS_FORMAT,
    dbt_key,
    stg_national_generation_mix,
    stg_national_intensity,
    stg_regional_intensity_generation_mix,
)
from transforms.core import build_fct_half_hourly_intensity
from transforms.marts import build_mart_daily_intensity

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


REGIONAL_RAW_COLUMNS = [
    "from",
    "to",
    "regionid",
    "dnoregion",
    "shortname",
    "intensity.forecast",
    "intensity.index",
    "fuel",
    "perc",
]


def _stg_inputs(spark):
    national_raw = spark.createDataFrame(
        [("2026-01-01T00:00Z", "2026-01-01T00:30Z", 105, 98, "low")],
        ["from", "to", "intensity.forecast", "intensity.actual", "intensity.index"],
    )
    regional_raw = spark.createDataFrame(
        [
            # region 1 has two fuel rows for the same period; forecasts differ on
            # purpose so we can see which one survives the dedup
            ("2026-01-01T00:00Z", "2026-01-01T00:30Z", 1, "SSE", "North Scotland", 50, "low", "wind", 80.0),
            ("2026-01-01T00:00Z", "2026-01-01T00:30Z", 1, "SSE", "North Scotland", 40, "low", "biomass", 20.0),
            ("2026-01-01T00:00Z", "2026-01-01T00:30Z", 2, "SPD", "South Scotland", 60, "low", "gas", 100.0),
        ],
        REGIONAL_RAW_COLUMNS,
    )  # fmt: skip
    return (
        stg_national_intensity(national_raw),
        stg_regional_intensity_generation_mix(regional_raw),
    )


def test_fct_half_hourly_intensity_unions_national_and_dedups_regional(spark):
    out = build_fct_half_hourly_intensity(*_stg_inputs(spark))
    rows = {r["region_id"]: r for r in out.collect()}

    assert out.columns == [
        "intensity_key",
        "date_key",
        "region_key",
        "period_start",
        "period_end",
        "period_date",
        "region_id",
        "intensity_forecast",
        "intensity_actual",
        "intensity_index",
    ]
    # one row per (period_start, region_id): national as region 0 + 2 regions
    assert out.count() == 3
    assert sorted(rows) == [0, 1, 2]

    # national keeps its actual; regional has no actual
    assert rows[0]["intensity_forecast"] == 105
    assert rows[0]["intensity_actual"] == 98
    assert rows[1]["intensity_actual"] is None

    # dbt: distinct on (period_start, region_id) ... order by fuel_type
    # -> 'biomass' sorts before 'wind', so its forecast wins
    assert rows[1]["intensity_forecast"] == 40

    # type check on period_date
    assert dict(out.dtypes)["period_date"] == "date"


def test_fct_half_hourly_intensity_keys_match_dbt(spark):
    out = build_fct_half_hourly_intensity(*_stg_inputs(spark))
    rows = {r["region_id"]: r for r in out.collect()}

    # generate_surrogate_key(['combined.period_start', 'combined.region_id'])
    assert rows[1]["intensity_key"] == _md5("2026-01-01 00:00:00-1")
    assert rows[0]["intensity_key"] == _md5("2026-01-01 00:00:00-0")

    # dim_date.date_key hashes date_day, which date_spine builds as
    # date + interval -> a Postgres timestamp, so it is md5('2026-01-01 00:00:00')
    # real value from dim_date in Postgres (checked 2026-11-07)
    assert rows[1]["date_key"] == "37c1bcdcf2b3dde7f748430b95fc4166"

    # dim_region.region_key: generate_surrogate_key(['region_id'])
    assert rows[1]["region_key"] == _md5("1")
    assert rows[0]["region_key"] == _md5("0")


def test_fct_half_hourly_intensity_on_fixture_has_912_rows(spark):
    national = stg_national_intensity(
        spark.read.parquet(str(FIXTURES / "national_intensity" / "2026-01-01.parquet"))
    )
    regional = stg_regional_intensity_generation_mix(
        spark.read.parquet(
            str(FIXTURES / "regional_intensity_generation_mix" / "2026-01-01.parquet")
        )
    )

    out = build_fct_half_hourly_intensity(national, regional)

    # 48 half-hour periods x (1 national + 18 regions)
    assert out.count() == 912
    assert out.select("intensity_key").distinct().count() == 912
    assert (
        out.filter(
            F.col("intensity_key").isNull()
            | F.col("date_key").isNull()
            | F.col("region_key").isNull()
        ).count()
        == 0
    )


FCT_INTENSITY_SCHEMA = (
    "period_date date, region_id int, date_key string, region_key string, "
    "intensity_forecast int, intensity_actual int, intensity_index string"
)


def test_mart_daily_intensity_aggregates_per_day_and_region(spark):
    day = datetime.date(2026, 1, 1)
    fct = spark.createDataFrame(
        [
            # region 0: two half-hours with actuals
            (day, 0, "dk-2026-01-01", "rk-0", 110, 100, "low"),
            (day, 0, "dk-2026-01-01", "rk-0", 190, 200, "moderate"),
            # region 1: two half-hours, regional has no actuals
            (day, 1, "dk-2026-01-01", "rk-1", 50, None, "low"),
            (day, 1, "dk-2026-01-01", "rk-1", 70, None, "low"),
        ],
        FCT_INTENSITY_SCHEMA,
    )

    out = build_mart_daily_intensity(fct)
    rows = {r["region_id"]: r for r in out.collect()}

    # dbt column order, minus dominant_index
    assert out.columns == [
        "daily_intensity_key",
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
    ]
    assert out.count() == 2

    assert rows[0]["periods_count"] == 2
    assert rows[0]["avg_intensity_forecast"] == pytest.approx(150)
    assert rows[0]["avg_intensity_actual"] == pytest.approx(150)
    assert rows[0]["min_intensity_actual"] == 100
    assert rows[0]["max_intensity_actual"] == 200
    # round(2 / 48 * 100, 1)
    assert rows[0]["intensity_actual_completeness_pct"] == 4.2

    # rows still count even when every actual is null
    assert rows[1]["periods_count"] == 2
    assert rows[1]["avg_intensity_forecast"] == pytest.approx(60)
    assert rows[1]["avg_intensity_actual"] is None
    assert rows[1]["intensity_actual_completeness_pct"] == 0.0

    # date_key / region_key are group-by columns, so they pass straight through
    assert rows[0]["date_key"] == "dk-2026-01-01"
    assert rows[0]["region_key"] == "rk-0"
    assert rows[1]["region_key"] == "rk-1"

    # period_date is a Postgres date here (not dim_date's timestamp),
    # so the key has no 00:00:00
    assert rows[0]["daily_intensity_key"] == _md5("2026-01-01-0")
    assert rows[1]["daily_intensity_key"] == _md5("2026-01-01-1")


def test_mart_daily_intensity_on_fixture_has_38_rows(spark):
    # reading the folder picks up both days (2026-01-01 and 2026-01-02)
    national = stg_national_intensity(
        spark.read.parquet(str(FIXTURES / "national_intensity"))
    )
    regional = stg_regional_intensity_generation_mix(
        spark.read.parquet(str(FIXTURES / "regional_intensity_generation_mix"))
    )

    out = build_mart_daily_intensity(
        build_fct_half_hourly_intensity(national, regional)
    )

    # 2 days x (1 national + 18 regions)
    assert out.count() == 38
    assert out.filter(F.col("periods_count") != 48).count() == 0

    national_days = out.filter(F.col("region_id") == 0).collect()
    assert len(national_days) == 2
    assert all(r["intensity_actual_completeness_pct"] == 100.0 for r in national_days)
