from sentence_transformers import SentenceTransformer

class EmbeddingModel:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def get_embedding(self, text: str):
        if text is None:
            text = ""

        embedding = self.model.encode(text)
        return [float(x) for x in embedding]