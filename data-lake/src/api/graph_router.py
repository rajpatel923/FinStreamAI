"""Graph API — company lookup, entity network, events."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter(prefix="/graph", tags=["graph"])


class CompanyIn(BaseModel):
    symbol: str
    name: str
    sector: str


class LinkEventIn(BaseModel):
    event_id: str
    symbol: str


class LinkArticleIn(BaseModel):
    article_id: str
    symbol: str


def _kg(request: Request):
    kg = getattr(request.app.state, "knowledge_graph", None)
    if kg is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")
    return kg


@router.get("/company/{symbol}")
async def get_company(symbol: str, request: Request, depth: int = Query(default=2, ge=1, le=4)) -> JSONResponse:
    kg = _kg(request)
    company = kg.get_company(symbol.upper())
    if company is None:
        raise HTTPException(status_code=404, detail=f"Company {symbol} not found")
    network = kg.get_company_network(symbol.upper(), depth=depth)
    return JSONResponse({"company": company, "network": network})


@router.post("/company")
async def upsert_company(body: CompanyIn, request: Request) -> JSONResponse:
    kg = _kg(request)
    result = kg.import_company(body.symbol.upper(), body.name, body.sector)
    return JSONResponse({"status": "ok", "company": result})


@router.post("/link/event")
async def link_event(body: LinkEventIn, request: Request) -> JSONResponse:
    kg = _kg(request)
    kg.link_event_to_company(body.event_id, body.symbol.upper())
    return JSONResponse({"status": "ok"})


@router.post("/link/article")
async def link_article(body: LinkArticleIn, request: Request) -> JSONResponse:
    kg = _kg(request)
    kg.link_article_to_company(body.article_id, body.symbol.upper())
    return JSONResponse({"status": "ok"})


@router.get("/affected/{event_id}")
async def affected_companies(event_id: str, request: Request) -> JSONResponse:
    kg = _kg(request)
    companies = kg.find_affected_companies(event_id)
    return JSONResponse({"event_id": event_id, "affected": companies})


@router.get("/pagerank")
async def pagerank(request: Request) -> JSONResponse:
    kg = _kg(request)
    scores = kg.pagerank()
    return JSONResponse({"pagerank": scores})


@router.get("/companies")
async def list_companies(request: Request) -> JSONResponse:
    kg = _kg(request)
    companies = kg.list_companies()
    return JSONResponse({"companies": companies})
