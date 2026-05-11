from RAG.retriever import TransactionRetriever


class TransactionRAGPipeline:
    def __init__(self, spark):
        self.retriever = TransactionRetriever(spark)

    def ask(self, question, top_k=5):
        results = self.retriever.retrieve(question, top_k)

        context = "\n\n".join(
            [
                f"Score: {round(item['score'], 4)}\n{item['content']}"
                for item in results
            ]
        )

        answer = f"""
Question:
{question}

Retrieved Gold Layer Context:
{context}

Answer:
Based on the retrieved Gold layer transaction records, these are the most relevant records for your question.
"""

        return answer