"""Unified cross-database query endpoint."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.query.unified_query import QuerySpec

router = APIRouter(prefix="/query", tags=["query"])


class QueryRequest(BaseModel):
    sources: list[str]
    filters: dict = {}
    limit: int = 100


@router.post("")
async def unified_query(body: QueryRequest, request: Request) -> JSONResponse:
    uq = getattr(request.app.state, "unified_query", None)
    if uq is None:
        raise HTTPException(status_code=503, detail="Query engine not initialized")

    spec = QuerySpec(sources=body.sources, filters=body.filters, limit=body.limit)
    try:
        results = uq.execute(spec)
        payload = {
            src: {"data": r.data, "error": r.error}
            for src, r in results.items()
        }
        return JSONResponse(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
