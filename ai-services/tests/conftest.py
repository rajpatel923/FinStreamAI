"""Shared fixtures for ai-services tests.

All external I/O (Redis, PostgreSQL, Kafka, HuggingFace, ChromaDB, XGBoost,
Anthropic) is mocked so that tests run offline and instantly.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ─── Shared sample data ──────────────────────────────────────────────────────

@pytest.fixture
def sample_returns() -> pd.Series:
    """250 normally-distributed daily returns (~SPY-like)."""
    rng = np.random.default_rng(42)
    r = rng.normal(loc=0.0005, scale=0.012, size=250)
    return pd.Series(r)


@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    """200 synthetic OHLCV bars derived from a random walk."""
    rng = np.random.default_rng(0)
    n = 200
    close = 100.0 * np.cumprod(1 + rng.normal(0.0003, 0.015, n))
    open_ = close * (1 + rng.normal(0, 0.002, n))
    high = np.maximum(open_, close) * (1 + rng.uniform(0, 0.005, n))
    low = np.minimum(open_, close) * (1 - rng.uniform(0, 0.005, n))
    volume = rng.integers(1_000_000, 5_000_000, n).astype(float)
    vwap = (open_ + close) / 2
    timestamps = pd.date_range("2024-01-01", periods=n, freq="1min")
    return pd.DataFrame(
        {"timestamp": timestamps, "open": open_, "high": high,
         "low": low, "close": close, "volume": volume, "vwap": vwap}
    )


# ─── Schema Registry patch (prevents httpx calls in avro_utils) ─────────────

@pytest.fixture(autouse=True)
def patch_schema_registry():
    with patch("src.utils.avro_utils.httpx.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"id": 1},
            raise_for_status=lambda: None,
        )
        yield mock_post
