# Real-Time Transaction Pipeline

A scalable, end-to-end Real-Time Transaction Data Pipeline built using Apache Kafka, PySpark Structured Streaming, Apache Airflow, and a Medallion Architecture (Bronze → Silver → Gold). The project also includes a REST API, Retrieval-Augmented Generation (RAG) components, configuration management, and automated workflow orchestration.

---

# Project Overview

This project demonstrates how modern data engineering pipelines process streaming transaction data in real time.

The pipeline performs the following tasks:

- Generates transaction events
- Publishes events to Kafka
- Consumes data using PySpark Structured Streaming
- Stores data using the Bronze → Silver → Gold architecture
- Schedules workflows using Apache Airflow
- Exposes services through a REST API
- Supports Retrieval-Augmented Generation (RAG)
- Includes automated tests and configuration management

---

# Architecture

```
                Transaction Data
                       │
                       ▼
              Apache Kafka Producer
                       │
                       ▼
                 Kafka Topic
                       │
                       ▼
         PySpark Structured Streaming
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
   Bronze Layer    Silver Layer     Gold Layer
 (Raw Data)      (Cleaned Data)   (Aggregated Data)
                       │
                       ▼
                  Analytics
                       │
                       ▼
                 REST API / RAG
```

---

# Project Structure

```
Realtime-Transaction-Pipeline
│
├── airflow_dags/
│   └── transaction_pipeline_dag.py
│
├── API/
│   ├── app.py
│   └── routes.py
│
├── Configs/
│   ├── airflow_config.json
│   ├── kafka_config.json
│   ├── spark_config.json
│   └── rag_config.json
│
├── data/
│   └── sample_orders.json
│
├── kafka_producer/
│   ├── producer.py
│   └── config.py
│
├── pyspark_streaming/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── schema.py
│
├── RAG/
│   ├── embedding.py
│   ├── rag_pipeline.py
│   ├── retriever.py
│   └── vector_store.py
│
├── tests/
│
├── utils/
│
├── docker-compose.yml
├── requirements.txt
└── main.py
```

---

# ⚙️ Technologies Used

| Technology | Purpose |
|------------|---------|
| Python | Programming Language |
| Apache Kafka | Real-time Event Streaming |
| PySpark | Stream Processing |
| Apache Spark | Distributed Processing |
| Apache Airflow | Workflow Scheduling |
| REST API | Data Access |
| Docker | Containerization |
| JSON | Configuration |
| Git & GitHub | Version Control |

---

# Pipeline Workflow

### Step 1

Generate transaction events.

↓

### Step 2

Publish events into Kafka topics.

↓

### Step 3

Consume streaming data using PySpark.

↓

### Step 4

Store raw records in the Bronze layer.

↓

### Step 5

Clean and validate data in the Silver layer.

↓

### Step 6

Aggregate business metrics in the Gold layer.

↓

### Step 7

Schedule the pipeline using Apache Airflow.

↓

### Step 8

Expose processed data through REST APIs and RAG components.

---

# Features

- Real-time transaction ingestion
- Kafka Producer implementation
- PySpark Structured Streaming
- Bronze, Silver, Gold Medallion Architecture
- Apache Airflow DAG
- REST API integration
- Retrieval-Augmented Generation (RAG)
- Configuration-driven pipeline
- Docker support
- Unit testing
- Modular project structure
- Scalable data engineering architecture

---

# Medallion Architecture

## Bronze Layer

- Stores raw streaming data
- No transformations
- Historical backup

## Silver Layer

- Cleans invalid records
- Data validation
- Removes duplicates
- Standardizes schema

## Gold Layer

- Business-ready data
- Aggregations
- KPIs
- Reporting datasets

---

# Getting Started

## Clone Repository

```bash
git clone https://github.com/gnavin478/Realtime-Transaction-Pipeline.git
```

## Navigate to Project

```bash
cd Realtime-Transaction-Pipeline
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Start Required Services

```bash
docker-compose up
```

## Run the Pipeline

```bash
python main.py
```

---

# Testing

Run unit tests using:

```bash
pytest
```

It motivates me to build more real-world Data Engineering projects.
