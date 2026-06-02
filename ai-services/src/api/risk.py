from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/risk", tags=["risk"])


class PortfolioRequest(BaseModel):
    positions: list[dict]  # [{"symbol": "AAPL", "weight": 0.5}, ...]


class BacktestRequest(BaseModel):
    symbol: str
    strategy: str = "buy_hold"


@router.get("/metrics/{symbol}")
def risk_metrics(symbol: str, request: Request) -> dict:
    """VaR, CVaR, Sharpe, MaxDD and more for a single symbol."""
    calculator = request.app.state.risk
    return calculator.compute_metrics(symbol.upper())


@router.post("/portfolio")
def portfolio_risk(req: PortfolioRequest, request: Request) -> dict:
    """Portfolio-level risk metrics across weighted positions."""
    calculator = request.app.state.risk
    return calculator.compute_portfolio_risk(req.positions)


@router.post("/backtest")
def backtest(req: BacktestRequest, request: Request) -> dict:
    """Simple buy-and-hold backtest for a symbol."""
    calculator = request.app.state.risk
    return calculator.backtest(req.symbol.upper(), req.strategy)
