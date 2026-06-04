"""Cache management endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter(prefix="/cache", tags=["cache"])


class WarmRequest(BaseModel):
    symbols: list[str]


def _cache(request: Request):
    cache = getattr(request.app.state, "cache", None)
    if cache is None:
        raise HTTPException(status_code=503, detail="Cache not initialized")
    return cache


@router.get("/stats")
async def cache_stats(request: Request) -> JSONResponse:
    cache = _cache(request)
    return JSONResponse(cache.get_stats())


@router.post("/warm")
async def warm_cache(body: WarmRequest, request: Request) -> JSONResponse:
    """Warm the cache for the provided symbols using TimescaleDB prices."""
    cache = _cache(request)
    timescale_dsn = getattr(request.app.state, "timescale_dsn", None)

    def fetch_price(symbol: str):
        if not timescale_dsn:
            return None
        try:
            import psycopg2
            conn = psycopg2.connect(timescale_dsn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT price FROM market_ticks WHERE symbol=%s ORDER BY time DESC LIMIT 1",
                    (symbol,),
                )
                row = cur.fetchone()
            conn.close()
            return float(row[0]) if row else None
        except Exception:
            return None

    loaded = cache.warm_prices(body.symbols, fetch_price)
    return JSONResponse({"status": "ok", "loaded": loaded})


@router.delete("/key/{key}")
async def invalidate_key(key: str, request: Request) -> JSONResponse:
    cache = _cache(request)
    deleted = cache.delete(key)
    return JSONResponse({"status": "ok", "deleted": deleted})
