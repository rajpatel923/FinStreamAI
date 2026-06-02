from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


class ChromaService:
    """sentence-transformers (all-MiniLM-L6-v2) + ChromaDB for semantic news search.

    Both model and ChromaDB client are lazy-loaded.  ChromaDB persists to disk
    so the index survives restarts without a separate infrastructure component.
    """

    def __init__(self, persist_dir: str = "./chroma_store") -> None:
        self._persist_dir = persist_dir
        self._model = None
        self._client = None
        self._collection = None

    def _get_model(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            logger.info("Loading sentence-transformers model (all-MiniLM-L6-v2)")
            self._model = SentenceTransformer("all-MiniLM-L6-v2")
            logger.info("Embeddings model loaded (384-dim)")
        return self._model

    def _get_collection(self):
        if self._collection is None:
            import chromadb

            self._client = chromadb.PersistentClient(path=self._persist_dir)
            self._collection = self._client.get_or_create_collection(
                "news_articles",
                metadata={"hnsw:space": "cosine"},
            )
        return self._collection

    def add_document(self, doc_id: str, text: str, metadata: dict | None = None) -> None:
        """Embed and store a document. Upserts if doc_id already exists."""
        model = self._get_model()
        collection = self._get_collection()
        embedding = model.encode([text])[0].tolist()
        collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            documents=[text],
            metadatas=[metadata or {}],
        )

    def search_similar(self, query: str, n_results: int = 5) -> list[dict]:
        """Return top-n most semantically similar documents.

        Returns list of {"id", "text", "metadata", "distance"} dicts.
        """
        collection = self._get_collection()
        if collection.count() == 0:
            return []
        model = self._get_model()
        query_embedding = model.encode([query])[0].tolist()
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=min(n_results, collection.count()),
        )
        return [
            {
                "id": results["ids"][0][i],
                "text": results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
                "distance": round(results["distances"][0][i], 4),
            }
            for i in range(len(results["ids"][0]))
        ]

    def count(self) -> int:
        return self._get_collection().count()
