from pyspark.sql import SparkSession
from pyspark.sql.functions import(
    col, sum, count, avg, current_timestamp, year, month, dayofmonth, quarter, date_format
)
from utils.logger import get_logger
from utils.constants import CUSTOMER_TABLE,CUSTOMER_CHECKPOINT,PRODUCT_TABLE,PRODUCT_CHECKPOINT,PAYMENT_TABLE,PAYMENT_CHECKPOINT,LOCATION_TABLE,LOCATION_CHECKPOINT,DATE_TABLE,DATE_CHECKPOINT,FACT_TRANSACTION_TABLE,FACT_TRANSACTION_CHECKPOINT
logger = get_logger("gold", "/Volumes/workspace/transaction_data_pipeline/pipeline_logs/gold_aggregation.log")

def create_spark_session():
    try:
        logger.info("Creating Spark session for Gold layer")
        spark = (
            SparkSession.builder
            .appName("SilverToGoldAggregation")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.exception(f"Error creating Spark session: {e}")
        raise

def read_silver_stream(spark):
    try:
        logger.info("Reading data from silver Layer data")
        silver_df = (
            spark.readStream
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("ignoreChanges", "true")
            .table("silver_transactions")
        )
        logger.info("Data read successfully from Silver layer")
        return silver_df
    except Exception as e:
        logger.exception(f"Error reading data from silver layer: {e}")
        raise

def create_dim_customer(silver_df):
    try:
        logger.info("Creating dim_customer table")
        dim_customer_df = (
            silver_df.select(
                col("customer_id"),
                col("customer_segment"),
                col("is_first_purchase")
            )
            .dropDuplicates(["customer_id"])
            .withColumn("Processed_at", current_timestamp())
        )
        logger.info("dim_customer table created successfully")
        return dim_customer_df
    except Exception as e:
        logger.exception(f"Error creating dim_customer table: {e}")
        raise

def create_dim_product(silver_df):
    try:
        logger.info("Creating dim_product")
        dim_product_df = (
            silver_df
            .select(
                col("product_id"),
                col("product_name"),
                col("unit_price"),
                col("cost_price")
            )
            .dropDuplicates(["product_id"])
            .withColumn("processed_at", current_timestamp())
        )
        logger.info("dim_product table created successfully")
        return dim_product_df
    except Exception as e:
        logger.exception(f"Error creating dim_product: {e}")
        raise

def create_dim_payment(silver_df):
    try:
        logger.info("Creating dim_payment")
        dim_payment_df = (
            silver_df
            .select(
                col("payment_method"),
                col("payment_status"),
                col("order_status"),
                col("delivery_status"),
                col("currency"),
                col("exchange_rate")
            )
            .dropDuplicates([
                "payment_method",
                "payment_status",
                "order_status",
                "delivery_status",
                "currency",
                "exchange_rate"
            ])
            .withColumn("processed_at", current_timestamp())
        )
        logger.info("dim_payment table created successfully")
        return dim_payment_df
    except Exception as e:
        logger.exception(f"Error creating dim_payment: {e}")
        raise

def create_dim_location(silver_df):
    try:
        logger.info("Creating dim_location")
        dim_location_df = (
            silver_df
            .select(
                col("city"),
                col("state"),
                col("country"),
                col("zip_code")
            )
            .dropDuplicates(["city", "state", "country", "zip_code"])
            .withColumn("processed_at", current_timestamp())
        )
        logger.info("dim_location table created successfully")
        return dim_location_df
    except Exception as e:
        logger.exception(f"Error creating dim_location: {e}")
        raise

def create_dim_date(silver_df):
    try:
        logger.info("Creating dim_date")
        dim_date_df = (
            silver_df
            .select(col("event_time"))
            .filter(col("event_time").isNotNull())
            .withColumn("full_date", col("event_time").cast("date"))
            .withColumn("year", year(col("event_time")))
            .withColumn("month", month(col("event_time")))
            .withColumn("day", dayofmonth(col("event_time")))
            .withColumn("quarter", quarter(col("event_time")))
            .withColumn("day_name", date_format(col("event_time"), "EEEE"))
            .dropDuplicates(["full_date"])
            .withColumn("processed_at", current_timestamp())
        )
        logger.info("dim_date table created successfully")
        return dim_date_df
    except Exception as e:
        logger.exception(f"Error creating dim_date: {e}")
        raise

def create_fact_transactions(silver_df):
    try:
        logger.info("Creating fact_transactions")
        fact_df = (
            silver_df
            .select(
                col("event_id"),
                col("transaction_id"),
                col("customer_id"),
                col("product_id"),
                col("payment_method"),
                col("payment_status"),
                col("order_status"),
                col("delivery_status"),
                col("currency"),
                col("exchange_rate"),
                col("city"),
                col("state"),
                col("country"),
                col("zip_code"),
                col("quantity"),
                col("unit_price"),
                col("cost_price"),
                col("gross_amount"),
                col("discount_amount"),
                col("tax_amount"),
                col("net_amount"),
                col("profit_amount"),
                col("refund_amount"),
                col("event_time"),
                col("ingestion_time"),
                col("processed_at")
            )
            .dropDuplicates(["event_id"])
        )
        logger.info("fact_df table created successfully")
        return fact_df
    except Exception as e:
        logger.exception(f"Error creating fact_transactions: {e}")
        raise

def write_gold_table(df, table_name, checkpoint_path):
    try:
        logger.info(f"Writing data to Gold table: {table_name}")

        query = (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(table_name)
        )

        query.awaitTermination()
        logger.info(f"Successfully written data to Gold table: {table_name}")

    except Exception as e:
        logger.exception(f"Error writing data to Gold table {table_name}: {e}")
        raise

def main():
    spark = create_spark_session()
    silver_df = read_silver_stream(spark)

    dim_customer_df = create_dim_customer(silver_df)
    dim_product_df = create_dim_product(silver_df)
    dim_payment_df = create_dim_payment(silver_df)
    dim_location_df = create_dim_location(silver_df)
    dim_date_df = create_dim_date(silver_df)
    fact_transactions_df = create_fact_transactions(silver_df)
 
    write_gold_table(
        dim_customer_df,
        CUSTOMER_TABLE,
        CUSTOMER_CHECKPOINT
    )

    write_gold_table(
        dim_product_df,
        PRODUCT_TABLE,
        PRODUCT_CHECKPOINT
    )

    write_gold_table(
        dim_payment_df,
        PAYMENT_TABLE,
        PAYMENT_CHECKPOINT
    )

    write_gold_table(
        dim_location_df,
        LOCATION_TABLE,
        LOCATION_CHECKPOINT
    )

    write_gold_table(
        dim_date_df,
        DATE_TABLE,
        DATE_CHECKPOINT
    )

    write_gold_table(
        fact_transactions_df,
        FACT_TRANSACTION_TABLE,
        FACT_TRANSACTION_CHECKPOINT
    )


if __name__ == "__main__":
    main()
