from utils.logger import get_logger
from kafka_producer.producer import run_producer
from pyspark_streaming.bronze.bronze_ingest import main as run_bronze
from pyspark_streaming.silver.silver_transform import main as run_silver

logger = get_logger("main_pipeline", "main_pipeline.log")


def run_pipeline():
    try:
        logger.info("Pipeline execution started")

        # Step 1: Produce sample transaction events to Kafka
        logger.info("Starting Kafka producer")
        run_producer(total_batches=5)
        logger.info("Kafka producer completed successfully")

        # Step 2: Read Kafka data and load into Bronze Delta table
        logger.info("Starting Bronze layer ingestion")
        run_bronze()
        logger.info("Bronze layer ingestion completed successfully")

        # Step 3: Read Bronze data and transform into Silver Delta table
        logger.info("Starting Silver layer transformation")
        run_silver()
        logger.info("Silver layer transformation completed successfully")

        logger.info("Pipeline execution finished successfully")

    except Exception as e:
        logger.exception(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()