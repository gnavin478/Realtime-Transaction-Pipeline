from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    current_timestamp
)
from utils.logger import get_logger
from utils.constants import SILVER_TABLE, SILVER_CHECKPOINT
logger = get_logger("silver", "/Volumes/workspace/transaction_data_pipeline/pipeline_logs/silver_transform.log")


def create_spark_session():
    try:
        logger.info("Creating Spark session for Silver layer")
        spark = (
            SparkSession.builder
            .appName("BronzeToSilverTransformation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.exception(f"Error creating Spark session: {e}")
        raise


def read_bronze_stream(spark):
    try:
        logger.info("Reading data from Bronze layer")
        bronze_df = (
            spark.readStream
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("ignoreChanges", "true")
            .table("bronze_transactions")
        )
        logger.info("Data read successfully from Bronze layer")
        return bronze_df
    except Exception as e:
        logger.exception(f"Error reading data from Bronze layer: {e}")
        raise


def transform_data(bronze_df):
    try:
        logger.info("Transforming data for Silver layer")

        silver_df = (
            bronze_df
            .filter(col("event_id").isNotNull())
            .filter(col("transaction_id").isNotNull())
            .filter(col("customer_id").isNotNull())
            .filter(col("merchant_id").isNotNull())
            .filter(col("product_id").isNotNull())
            .filter(col("product_name").isNotNull())
            .filter(col("quantity").isNotNull())
            .filter(col("unit_price").isNotNull())
            .filter(col("gross_amount").isNotNull())
            .filter(col("net_amount").isNotNull())
            .filter(col("payment_method").isNotNull())
            .filter(col("city").isNotNull())
            .filter(col("event_time").isNotNull())
            .filter(col("quantity") > 0)
            .filter(col("unit_price") > 0)
            .filter(col("gross_amount") >= 0)
            .filter(col("net_amount") >= 0)
            .filter((col("discount_amount").isNull()) | (col("discount_amount") >= 0))
            .filter((col("tax_amount").isNull()) | (col("tax_amount") >= 0))
            .withColumn("product_name", trim(col("product_name")))
            .withColumn("payment_method", upper(trim(col("payment_method"))))
            .withColumn("city", trim(col("city")))
            .withColumn("device_type", upper(trim(col("device_type"))))
            .withColumn("event_type", upper(trim(col("event_type"))))
            .withColumn("transaction_status", upper(trim(col("transaction_status"))))
            .withColumn("payment_status", upper(trim(col("payment_status"))))
            .withColumn("order_status", upper(trim(col("order_status"))))
            .withColumn("delivery_status", upper(trim(col("delivery_status"))))
            .withColumn("customer_segment", upper(trim(col("customer_segment"))))
            .withColumn("is_first_purchase", upper(trim(col("is_first_purchase"))))
            .withColumn("currency", upper(trim(col("currency"))))
            .withColumn("state", trim(col("state")))
            .withColumn("country", trim(col("country")))
            .withColumn("zip_code", trim(col("zip_code")))
            .withColumn("session_id", trim(col("session_id")))
            .withColumn("event_time", to_timestamp(col("event_time")))
            .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
            .withColumn("processed_at", current_timestamp())
            .dropDuplicates(["event_id"])
        )

        logger.info("Transformation completed successfully")
        return silver_df

    except Exception as e:
        logger.exception(f"Error transforming data for Silver layer: {e}")
        raise


def write_to_silver(silver_df):
    try:
        logger.info("Writing data to Silver layer")

        query = (
            silver_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", SILVER_CHECKPOINT)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(SILVER_TABLE)
        )

        logger.info("Silver streaming query started successfully")
        query.awaitTermination()

    except Exception as e:
        logger.exception(f"Error writing data to Silver layer: {e}")
        raise

def main():
    spark = create_spark_session()
    bronze_df = read_bronze_stream(spark)
    silver_df = transform_data(bronze_df)
    write_to_silver(silver_df)


if __name__ == "__main__":
    main()