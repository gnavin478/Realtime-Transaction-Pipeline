from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

transaction_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),

    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("gross_amount", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("net_amount", DoubleType(), True),

    StructField("payment_method", StringType(), True),
    StructField("city", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("transaction_status", StringType(), True),

    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),  

    StructField("currency", StringType(), True),
    StructField("exchange_rate", DoubleType(), True),
    StructField("profit_amount", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),

    StructField("order_status", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("delivery_status", StringType(), True),
    StructField("refund_amount", DoubleType(), True),

    StructField("customer_segment", StringType(), True),
    StructField("is_first_purchase", StringType(), True),
    StructField("session_id", StringType(), True),

    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zip_code", StringType(), True),
])