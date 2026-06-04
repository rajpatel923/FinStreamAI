"""Gold layer — aggregates and joins silver data into feature/analytics tables."""
from __future__ import annotations

import datetime

import pandas as pd
import structlog

from src.lake.delta_client import DeltaClient

logger = structlog.get_logger(__name__)


class GoldLayer:
    """Reads silver tables, builds gold-level aggregations."""

    def __init__(
        self,
        delta_client: DeltaClient,
        silver_base: str,
        gold_base: str,
    ) -> None:
        self._client = delta_client
        self._silver = silver_base.rstrip("/")
        self._gold = gold_base.rstrip("/")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _read_silver(self, data_type: str) -> pd.DataFrame:
        path = f"{self._silver}/{data_type}"
        if not self._client.table_exists(path):
            return pd.DataFrame()
        return self._client.read(path)

    def _write_gold(self, name: str, df: pd.DataFrame, partition_by: list[str] | None = None) -> None:
        path = f"{self._gold}/{name}"
        self._client.write(path, df, mode="overwrite", partition_by=partition_by, schema_mode="overwrite")
        logger.info("Gold write", name=name, rows=len(df))

    # ------------------------------------------------------------------
    # Features
    # ------------------------------------------------------------------
    def build_features(self) -> pd.DataFrame:
        """Join silver market + silver sentiment + events on symbol+time window."""
        market_df = self._read_silver("market_tick")
        news_df = self._read_silver("news_article")

        if market_df.empty:
            logger.warning("No silver market data available for gold features")
            return pd.DataFrame()

        # Compute price-based features per symbol
        if "price" in market_df.columns and "symbol" in market_df.columns:
            market_df["price"] = pd.to_numeric(market_df["price"], errors="coerce")
            features = (
                market_df.groupby("symbol")["price"]
                .agg(price_mean="mean", price_std="std", price_min="min", price_max="max", tick_count="count")
                .reset_index()
            )
        else:
            features = pd.DataFrame()

        # Attach sentiment if available
        if not news_df.empty and "sentiment_score" in news_df.columns and "symbol" in news_df.columns:
            news_df["sentiment_score"] = pd.to_numeric(news_df["sentiment_score"], errors="coerce")
            sentiment_agg = news_df.groupby("symbol")["sentiment_score"].mean().reset_index()
            sentiment_agg.columns = ["symbol", "avg_sentiment"]
            if not features.empty:
                features = features.merge(sentiment_agg, on="symbol", how="left")

        if not features.empty:
            features["_computed_at"] = datetime.datetime.utcnow().isoformat()
            self._write_gold("features", features, partition_by=None)

        return features

    # ------------------------------------------------------------------
    # Signals
    # ------------------------------------------------------------------
    def build_signals(self) -> pd.DataFrame:
        """Aggregate price signals: momentum, volatility."""
        market_df = self._read_silver("market_tick")
        if market_df.empty:
            return pd.DataFrame()

        if "price" not in market_df.columns or "symbol" not in market_df.columns:
            return pd.DataFrame()

        market_df["price"] = pd.to_numeric(market_df["price"], errors="coerce")
        market_df = market_df.dropna(subset=["price"])

        signals_rows = []
        for symbol, grp in market_df.groupby("symbol"):
            grp = grp.sort_values("timestamp") if "timestamp" in grp.columns else grp
            prices = grp["price"].values
            if len(prices) < 2:
                continue
            momentum = float(prices[-1] - prices[0])
            volatility = float(grp["price"].std())
            signals_rows.append(
                {
                    "symbol": symbol,
                    "momentum": momentum,
                    "volatility": volatility,
                    "latest_price": float(prices[-1]),
                    "_computed_at": datetime.datetime.utcnow().isoformat(),
                }
            )

        signals_df = pd.DataFrame(signals_rows)
        if not signals_df.empty:
            self._write_gold("signals", signals_df, partition_by=None)
        return signals_df

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------
    def build_analytics(self) -> pd.DataFrame:
        """Build pre-aggregated analytics: news volume, social engagement."""
        news_df = self._read_silver("news_article")
        social_df = self._read_silver("social_post")

        rows = []
        if not news_df.empty and "symbol" in news_df.columns:
            for symbol, grp in news_df.groupby("symbol"):
                rows.append(
                    {
                        "symbol": symbol,
                        "news_count": len(grp),
                        "avg_sentiment": float(pd.to_numeric(grp.get("sentiment_score", pd.Series(dtype=float)), errors="coerce").mean())
                        if "sentiment_score" in grp.columns
                        else None,
                        "_computed_at": datetime.datetime.utcnow().isoformat(),
                    }
                )

        analytics_df = pd.DataFrame(rows)
        if not analytics_df.empty:
            self._write_gold("analytics", analytics_df, partition_by=None)
        return analytics_df
