import datetime

from pyspark.sql import functions as F


def test_late_utc_half_hour_stays_on_same_date(spark):
    df = spark.createDataFrame([("2025-01-01T23:30Z",)], ["raw_timestamp"])
    df = df.withColumn(
        "date", F.to_date(F.to_timestamp("raw_timestamp", "yyyy-MM-dd'T'HH:mmX"))
    )
    result = df.collect()[0]["date"]
    assert result == datetime.date(2025, 1, 1)
