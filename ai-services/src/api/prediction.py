from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/predict", tags=["prediction"])


class BatchRequest(BaseModel):
    symbols: list[str]


@router.get("/signals/{symbol}")
def get_signal(symbol: str, request: Request) -> dict:
    """Get the latest XGBoost directional prediction for a symbol."""
    predictor = request.app.state.predictor
    return predictor.predict(symbol.upper())


@router.post("/batch")
def batch_predict(req: BatchRequest, request: Request) -> dict:
    """Predict direction for multiple symbols at once."""
    predictor = request.app.state.predictor
    results = predictor.predict_batch([s.upper() for s in req.symbols])
    return {"results": results, "count": len(results)}


@router.get("/shap/{symbol}")
def shap_values(symbol: str, request: Request) -> dict:
    """SHAP feature importance values for the symbol's prediction model."""
    predictor = request.app.state.predictor
    return predictor.get_shap_values(symbol.upper())
