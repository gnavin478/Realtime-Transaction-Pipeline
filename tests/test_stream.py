from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("TestPipeline")
        .getOrCreate()
    )


def test_remove_null_transaction_id():
    spark = get_spark()

    data = [
        ("ABC1", 100.0),
        (None, 200.0),
        ("ABC2", 300.0)
    ]

    df = spark.createDataFrame(data, ["transaction_id", "amount"])
    result_df = df.filter(col("transaction_id").isNotNull())

    assert result_df.count() == 2


def test_remove_duplicate_transactions():
    spark = get_spark()

    data = [
        ("ABC1", 100.0),
        ("ABC1", 100.0),
        ("ABC2", 200.0)
    ]

    df = spark.createDataFrame(data, ["transaction_id", "amount"])
    result_df = df.dropDuplicates(["transaction_id"])

    assert result_df.count() == 2


def test_event_time_conversion():
    spark = get_spark()

    data = [
        ("ABC1", "2026-04-17 10:30:00"),
        ("ABC2", "2026-04-17 11:00:00")
    ]

    df = spark.createDataFrame(data, ["transaction_id", "event_time"])
    result_df = df.withColumn("event_time", to_timestamp(col("event_time")))

    assert dict(result_df.dtypes)["event_time"] == "timestamp"