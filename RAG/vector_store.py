from pyspark.sql.functions import col, concat_ws, lit, current_timestamp, udf
from pyspark.sql.types import ArrayType, FloatType
from RAG.embedding import EmbeddingModel

GOLD_FACT_TABLE = "fact_transactions"

VECTOR_STORE_PATH = "/Volumes/workspace/transaction_data_pipeline/rag/tables/transaction_vector_store"


def build_vector_store(spark):
    embedding_model = EmbeddingModel()

    def generate_embedding(text):
        return embedding_model.get_embedding(text)

    embedding_udf = udf(generate_embedding, ArrayType(FloatType()))

    fact_df = spark.table(GOLD_FACT_TABLE)

    rag_df = (
        fact_df
        .withColumn(
            "content",
            concat_ws(
                " ",
                lit("Transaction ID:"), col("transaction_id").cast("string"),
                lit("Customer ID:"), col("customer_id").cast("string"),
                lit("Product ID:"), col("product_id").cast("string"),
                lit("Payment Method:"), col("payment_method").cast("string"),
                lit("Payment Status:"), col("payment_status").cast("string"),
                lit("Order Status:"), col("order_status").cast("string"),
                lit("Delivery Status:"), col("delivery_status").cast("string"),
                lit("City:"), col("city").cast("string"),
                lit("State:"), col("state").cast("string"),
                lit("Country:"), col("country").cast("string"),
                lit("Quantity:"), col("quantity").cast("string"),
                lit("Net Amount:"), col("net_amount").cast("string"),
                lit("Profit Amount:"), col("profit_amount").cast("string"),
                lit("Refund Amount:"), col("refund_amount").cast("string"),
                lit("Event Time:"), col("event_time").cast("string")
            )
        )
        .select(
            col("transaction_id").cast("string").alias("doc_id"),
            col("content")
        )
        .dropDuplicates(["doc_id"])
    )

    vector_df = (
        rag_df
        .withColumn("embedding", embedding_udf(col("content")))
        .withColumn("source_table", lit(GOLD_FACT_TABLE))
        .withColumn("created_at", current_timestamp())
    )

    (
        vector_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(VECTOR_STORE_PATH)
    )

    print("Vector store created successfully")
    print(f"Path: {VECTOR_STORE_PATH}")