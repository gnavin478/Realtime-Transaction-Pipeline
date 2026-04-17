# =========================
# TABLE NAMES
# =========================
BRONZE_TABLE = "bronze_transactions"
SILVER_TABLE = "silver_transactions"
CUSTOMER_TABLE = "dim_customer"
PRODUCT_TABLE = "dim_product"
PAYMENT_TABLE = "dim_payment"
LOCATION_TABLE = "dim_location"
DATE_TABLE = "dim_date"
FACT_TRANSACTION_TABLE = "fact_transactions"

# =========================
# CHECKPOINT PATHS (Databricks Volume)
# =========================
BRONZE_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/bronze/checkpoints/bronze_transactions"
SILVER_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/silver/checkpoints/silver_transactions_v3"
CUSTOMER_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/dim_customer"
PRODUCT_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/dim_product"
PAYMENT_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/dim_payment"
LOCATION_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/dim_location"
DATE_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/dim_date"
FACT_TRANSACTION_CHECKPOINT = "/Volumes/workspace/transaction_data_pipeline/gold/checkpoints/fact_transactions"
