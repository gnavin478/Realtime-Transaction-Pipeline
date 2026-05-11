import numpy as np

from RAG.embedding import EmbeddingModel


VECTOR_STORE_PATH = "/Volumes/workspace/transaction_data_pipeline/rag/tables/transaction_vector_store"


def cosine_similarity(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)

    if np.linalg.norm(vec1) == 0 or np.linalg.norm(vec2) == 0:
        return 0.0

    return float(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))


class TransactionRetriever:
    def __init__(self, spark):
        self.spark = spark
        self.embedding_model = EmbeddingModel()

    def retrieve(self, question, top_k=5):
        question_embedding = self.embedding_model.get_embedding(question)

        vector_df = (
            self.spark.read
            .format("delta")
            .load(VECTOR_STORE_PATH)
            .select("doc_id", "content", "embedding")
        )

        rows = vector_df.limit(10000).collect()

        results = []

        for row in rows:
            score = cosine_similarity(question_embedding, row["embedding"])

            results.append({
                "doc_id": row["doc_id"],
                "content": row["content"],
                "score": score
            })

        results = sorted(results, key=lambda x: x["score"], reverse=True)

        return results[:top_k]