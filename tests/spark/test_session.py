import datetime
from pyspark.sql import functions as F

def test_timezone(spark):
    df = spark.createDataFrame([("2025-01-01T23:30Z",)], ["timestamp"])
    df = df.withColumn(
        "date",
        F.to_date(F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mmX"), "yyyy-MM-dd")
    )
    result = df.collect()[0]["date"]
    assert result == datetime.date(2025, 1, 1)