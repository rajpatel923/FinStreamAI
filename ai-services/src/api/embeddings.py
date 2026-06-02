from __future__ import annotations

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel

router = APIRouter(prefix="/search", tags=["embeddings"])


class AddDocumentRequest(BaseModel):
    doc_id: str
    text: str
    metadata: dict | None = None


@router.get("/similar")
def search_similar(
    q: str = Query(..., description="Search query"),
    n: int = Query(5, ge=1, le=20, description="Number of results"),
    request: Request = None,
) -> dict:
    """Return top-n semantically similar news articles."""
    chroma = request.app.state.chroma
    results = chroma.search_similar(q, n_results=n)
    return {"query": q, "results": results, "count": len(results)}


@router.post("/documents")
def add_document(req: AddDocumentRequest, request: Request) -> dict:
    """Embed and store a document in the ChromaDB index."""
    chroma = request.app.state.chroma
    chroma.add_document(req.doc_id, req.text, req.metadata)
    return {"status": "ok", "doc_id": req.doc_id, "total": chroma.count()}
