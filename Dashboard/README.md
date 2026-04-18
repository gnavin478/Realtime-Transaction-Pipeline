# Realtime Transaction Dashboard (Databricks)

This dashboard presents **real-time business insights** generated from a streaming data pipeline built using Kafka, PySpark, and Delta Lake.
It is designed to provide a clear view of transaction performance across time, location, customer segments, and payment behavior.

---

## Objective

The goal of this dashboard is to transform raw streaming transaction data into **actionable business insights** using a structured **Medallion Architecture (Bronze → Silver → Gold)** and visualize them using **Databricks SQL Dashboards**.

---

## Data Architecture

The dashboard is built on top of the **Gold layer**, which follows a **star schema design**:

* **Fact Table**

  * `fact_transactions` → transactional metrics (revenue, profit, quantity, etc.)

* **Dimension Tables**

  * `dim_customer`
  * `dim_product`
  * `dim_payment`
  * `dim_location`
  * `dim_date`

---

## Key Metrics

The dashboard tracks the following KPIs:

* Total Transactions
* Total Revenue
* Total Profit
* Total Refund Amount
* Average Order Value

---

## Dashboard Visualizations

### 1. Revenue Trend (Line Chart)

* Displays daily revenue over time
* Helps identify growth patterns and seasonality

### 2. City-wise Performance (Bar Chart)

* Shows top-performing cities by revenue
* Enables regional analysis

### 3. Payment Method Distribution (Pie Chart)

* Breakdown of transactions by payment method
* Useful for understanding customer preferences

### 4. Product Performance (Bar Chart)

* Highlights top-selling products
* Based on revenue and quantity

### 5. Order & Delivery Status (Stacked Bar Chart)

* Tracks order lifecycle (completed, pending, failed)
* Monitors delivery performance

### 6. Customer Segmentation (Grouped Bar Chart)

* Compares revenue across customer segments
* Includes first-time vs returning customers

### 7. Time-based Analysis (Line Chart)

* Revenue and transaction trends across date hierarchy
* Supports monthly and quarterly insights

### 8. Location Analysis (Bar Chart)

* Revenue distribution across cities/states/countries
* Identifies high-performing regions

---

## Data Source

All dashboard visualizations are powered by **Gold layer tables** derived from streaming data:

* Source: Kafka (real-time transactions)
* Processing: PySpark Structured Streaming
* Storage: Delta Lake (Databricks)

---

## Business Value

This dashboard enables:

* Real-time monitoring of transaction performance
* Identification of top-performing regions and products
* Analysis of customer behavior and payment trends
* Faster decision-making using aggregated insights

---

## Repository Structure

```text
dashboard/
├── queries/          # SQL queries used for visualizations
├── screenshots/      # Dashboard preview images
└── README.md         # Documentation
```

---

## Tools & Technologies

* Databricks SQL
* Apache Spark (PySpark)
* Delta Lake
* Apache Kafka
* Medallion Architecture

