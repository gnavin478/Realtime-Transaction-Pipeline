from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = "/Workspace/Repos/gnavin478@gmail.com/Realtime-Transaction-Pipeline"
from utils.logger import get_logger
logger = get_logger("pyspark_streaming/gold", "gold_aggregation.log")

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from kafka_producer import kafkaProducer
from pyspark_streaming.bronze import bronze_ingest
from pyspark_streaming.silver import silver_transform
from pyspark_streaming.gold import gold_aggregation

def validate_project_paths():
    logger.info("Validating Project Paths...")

    required_path[
        PROJECT_ROOT,
        os.path.join(PROJECT_ROOT, "pyspark_streaming"),
        os.path.join(PROJECT_ROOT, "kafka_producer"),
        os.path.join(PROJECT_ROOT, "Configs")
    ]

     missing = [p for p in required_paths if not os.path.exists(p)]

    if missing:
        raise FileNotFoundError(f"Missing paths: {missing}")

    print("All paths validated successfully")

def run_producer():
    logger.info("Starting Kafka Producer")
    try:
        kafkaProducer.run_producer()
        logger.info("Kafka Producer completed successfully")
    except Exception as e:
        logger.error(f"Error in Kafka Producer: {e}")
        raise e

def run_bronze():
    logger.info("Starting Bronze Ingestion")
    try:
        bronze_ingest.main()
        logger.info("Bronze Ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error in Bronze Ingestion: {e}")
        raise e

def run_silver():
    logger.info("Starting Silver Transformation")
    try:
        silver_transform.main()
        logger.info("Silver Transformation completed successfully")
    except Exception as e:
        logger.error(f"Error in Silver Transformation: {e}")
        raise e

def run_gold():
    logger.info("Starting Gold Aggregation")
    try:
        gold_aggregation.main()
        logger.info("Gold Aggregation completed successfully")
    except Exception as e:
        logger.error(f"Error in Gold Aggregation: {e}")
        raise e

default_args = {
    "owner": "gnavin",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Realtime-Transaction-Pipeline",
    description="Kafka -> Bronze -> Silver -> Gold Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 14),
    schedule_interval=None,
    catchup=False,
    tags=["kafka", "spark", "airflow"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_task = PythonOperator(
        task_id = "validate_paths",
        python_callable = validate_project_paths 
    )

    producer_task = PythonOperator(
        task_id = "run_producer",
        python_callable = run_producer
    )

    bronze_task = PythonOperator(
        task_id = "run_bronze",
        python_callable = run_bronze
    )

    silver_task = PythonOperator(
        task_id = "run_silver",
        python_callable = run_silver
    )

    gold_task = PythonOperator(
        task_id = "run_gold",
        python_callable = run_gold
    )

    end = EmptyOperator(task_id="end")

# =========================================================
# DAG FLOW
# =========================================================

start >> validate_task >> producer_task >> bronze_task >> silver_task >> gold_task >> end
