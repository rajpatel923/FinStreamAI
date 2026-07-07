"""Analytics endpoints: portfolio, risk, backtest."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

import httpx
import pandas as pd
import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.database import get_timescale_db
from src.core.dependencies import get_current_user, require_role
from src.models.user import User

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/portfolio")
async def get_portfolio(
    symbols: str = Query(..., description="Comma-separated symbols"),
    period: str = Query(default="30d", pattern=r"^\d+[dw]$"),
    user: User = Depends(require_role("premium_user", "admin")),
    db: AsyncSession = Depends(get_timescale_db),
):
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    days = _parse_period(period)
    from datetime import timedelta

    from_ts = datetime.now(timezone.utc) - timedelta(days=days)

    portfolio_data: dict[str, Any] = {}
    for sym in symbol_list:
        try:
            sql = text(
                """
                SELECT timestamp, close
                FROM market_bars_1hour
                WHERE symbol = :symbol AND timestamp >= :from_ts
                ORDER BY timestamp ASC
                LIMIT 5000
                """
            )
            result = await db.execute(sql, {"symbol": sym, "from_ts": from_ts})
            rows = result.mappings().all()
        except Exception as exc:
            logger.warning("Portfolio query failed", symbol=sym, error=str(exc))
            rows = []
        if rows:
            prices = [float(r["close"]) for r in rows if r["close"] is not None]
            if len(prices) >= 2:
                returns = pd.Series(prices).pct_change().dropna()
                portfolio_data[sym] = {
                    "return_pct": round((prices[-1] / prices[0] - 1) * 100, 4),
                    "volatility": round(float(returns.std()) * 100, 4),
                    "current_price": prices[-1],
                    "data_points": len(prices),
                }

    return {"symbols": symbol_list, "period": period, "portfolio": portfolio_data}


@router.get("/risk")
async def get_risk(
    symbols: str = Query(..., description="Comma-separated symbols"),
    user: User = Depends(get_current_user),
):
    """Delegates to ai-services risk endpoint."""
    is_premium = user.role in ("premium_user", "admin")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{settings.AI_SERVICES_URL}/api/v1/risk/calculate",
                params={"symbols": symbols, "full": str(is_premium).lower()},
                headers={"X-User-Id": str(user.id), "X-User-Role": user.role},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"AI services unavailable: {e}",
        )


@router.post("/backtest")
async def run_backtest(
    body: dict[str, Any],
    user: User = Depends(require_role("premium_user", "admin")),
    db: AsyncSession = Depends(get_timescale_db),
):
    """Vectorized pandas backtest over market_bars."""
    symbol = body.get("symbol", "AAPL").upper()
    strategy = body.get("strategy", "sma_crossover")
    short_window = int(body.get("short_window", 10))
    long_window = int(body.get("long_window", 50))
    initial_capital = float(body.get("initial_capital", 10000.0))

    from_ts_raw = body.get("from_ts")
    to_ts_raw = body.get("to_ts")
    from datetime import timedelta

    from_ts = datetime.fromisoformat(from_ts_raw) if from_ts_raw else datetime.now(timezone.utc) - timedelta(days=30)
    to_ts = datetime.fromisoformat(to_ts_raw) if to_ts_raw else datetime.now(timezone.utc)

    try:
        sql = text(
            """
            SELECT timestamp, close
            FROM market_bars
            WHERE symbol = :symbol AND timeframe = '1min'
              AND timestamp >= :from_ts AND timestamp <= :to_ts
            ORDER BY timestamp ASC
            LIMIT 50000
            """
        )
        result = await db.execute(sql, {"symbol": symbol, "from_ts": from_ts, "to_ts": to_ts})
        rows = result.mappings().all()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Data unavailable: {exc}",
        )
    if len(rows) < long_window + 5:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Insufficient data for backtest")

    df = pd.DataFrame([dict(r) for r in rows])
    df["close"] = df["close"].astype(float)
    df["short_ma"] = df["close"].rolling(short_window).mean()
    df["long_ma"] = df["close"].rolling(long_window).mean()
    df["signal"] = 0
    df.loc[df["short_ma"] > df["long_ma"], "signal"] = 1
    df.loc[df["short_ma"] <= df["long_ma"], "signal"] = -1
    df["position"] = df["signal"].shift(1).fillna(0)
    df["returns"] = df["close"].pct_change()
    df["strategy_returns"] = df["position"] * df["returns"]
    df["cumulative"] = (1 + df["strategy_returns"]).cumprod()
    final_value = initial_capital * float(df["cumulative"].iloc[-1])
    total_return = (final_value / initial_capital - 1) * 100
    sharpe = float(df["strategy_returns"].mean() / df["strategy_returns"].std()) * (252**0.5) if df["strategy_returns"].std() > 0 else 0.0

    return {
        "symbol": symbol,
        "strategy": strategy,
        "initial_capital": initial_capital,
        "final_value": round(final_value, 2),
        "total_return_pct": round(total_return, 4),
        "sharpe_ratio": round(sharpe, 4),
        "num_bars": len(df),
        "from_ts": from_ts.isoformat(),
        "to_ts": to_ts.isoformat(),
    }


def _parse_period(period: str) -> int:
    unit = period[-1]
    n = int(period[:-1])
    return n * 7 if unit == "w" else n
