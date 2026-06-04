"""Lake API — quality reports, partition stats, ingest trigger."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/lake", tags=["lake"])


@router.get("/quality")
async def quality_report(request: Request) -> JSONResponse:
    """Return quality report from the quarantine tracker."""
    quarantine = getattr(request.app.state, "quarantine", None)
    if quarantine is None:
        raise HTTPException(status_code=503, detail="Quarantine not initialized")
    return JSONResponse(quarantine.quality_report())


@router.get("/partitions")
async def partition_stats(request: Request) -> JSONResponse:
    """Return partition stats from the data catalog."""
    catalog = getattr(request.app.state, "catalog", None)
    if catalog is None:
        raise HTTPException(status_code=503, detail="Catalog not initialized")
    try:
        stats = catalog.get_stats()
        return JSONResponse({"partitions": stats})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/ingest/silver")
async def trigger_silver(request: Request) -> JSONResponse:
    """Trigger a silver-layer processing run."""
    silver = getattr(request.app.state, "silver_layer", None)
    if silver is None:
        raise HTTPException(status_code=503, detail="Silver layer not initialized")
    try:
        counts = {
            "market_data": silver.process_market_data(),
            "news": silver.process_news(),
            "social": silver.process_social(),
        }
        return JSONResponse({"status": "ok", "processed": counts})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/ingest/gold")
async def trigger_gold(request: Request) -> JSONResponse:
    """Trigger a gold-layer aggregation run."""
    gold = getattr(request.app.state, "gold_layer", None)
    if gold is None:
        raise HTTPException(status_code=503, detail="Gold layer not initialized")
    try:
        features = gold.build_features()
        signals = gold.build_signals()
        analytics = gold.build_analytics()
        return JSONResponse({
            "status": "ok",
            "features_rows": len(features),
            "signal_rows": len(signals),
            "analytics_rows": len(analytics),
        })
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
