from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark_streaming.schema import transaction_schema
from kafka_producer.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISM,
    KAFKA_USERNAME,
    KAFKA_PASSWORD,
    KAFKA_TOPIC,
    EVENTS_PER_BATCH,
    SLEEP_SECONDS
)

from utils.logger import get_logger
logger = get_logger("pyspark_streaming/bronze", "bronze_ingest.log")

def create_spark_session():
    try:
        logger.info("Creating spark session")
        spark = (
            SparkSession.builder
            .appName("kafkaToDeltaBronze")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating spark session: {e}")
        raise e

def read_from_kafka(spark):
    try:
        logger.info("Reading from kafka")
        kafka_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
            .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
            .option(
                "kafka.sasl.jaas.config",
                f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
            )
            .option("startingOffsets", "earliest")
            .load()
        )
        logger.info("Read data from kafka successfully")
        return kafka_df
    except Exception as e:
        logger.error(f"Error reading from kafka: {e}")
        raise

def parse_kafka_data(kafka_df):
    try:
        logger.info("Parsing kafka data")

        json_df = kafka_df.selectExpr(
            "CAST(value AS STRING) AS json_value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp"
        )

        parsed_df = json_df.withColumn(
            "data",
            from_json(col("json_value"), transaction_schema)
        )

        final_df = parsed_df.selectExpr(
            "data.*",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp"
        )

        logger.info("Kafka data parsed successfully")
        return final_df

    except Exception as e:
        logger.error(f"Error parsing kafka data: {e}")
        raise

def write_to_delta(parsed_df):
    def process_batch(batch_df, batch_id):
        try:
            logger.info(f"Processing batch_id={batch_id}")

            total_count = batch_df.count()
            logger.info(f"Batch {batch_id} record count: {total_count}")

            invalid_count = batch_df.filter(col("event_id").isNull()).count()
            logger.info(f"Batch {batch_id} invalid record count: {invalid_count}")

            valid_df = batch_df.filter(col("event_id").isNotNull())

            sample_rows = valid_df.limit(5).toJSON().collect()
            for row in sample_rows:
                logger.info(f"Batch {batch_id} sample record: {row}")

            valid_df.write.format("delta").mode("append").save("data/delta/bronze_transactions")

            logger.info(f"Batch {batch_id} successfully written to Delta")

        except Exception as e:
            logger.exception(f"Error while processing batch {batch_id}: {e}")
            raise

    try:
        logger.info("Starting write stream to Delta")
        query = (
            parsed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .option(
                "checkpointLocation",
                "/Volumes/workspace/transaction_data_pipeline/bronze/checkpoints/bronze_transactions"
            )
            .toTable("bronze_transactions")
        )

        logger.info("Streaming query started successfully")
        query.awaitTermination()

    except Exception as e:
        logger.exception(f"Error while starting streaming write: {e}")
        raise

def main():
    spark = create_spark_session()
    kafka_df = read_from_kafka(spark)
    parsed_df = parse_kafka_data(kafka_df)
    write_to_delta(parsed_df)

if __name__ == "__main__":
    main()