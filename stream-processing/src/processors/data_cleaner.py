import time

import structlog

from src.config import settings
from src.state.rolling_state import RollingState

logger = structlog.get_logger(__name__)

# Maximum age a tick can have (in ms) before it's rejected as stale
_MAX_TICK_AGE_MS = 5_000
_ZSCORE_OUTLIER_THRESHOLD = 3.0


class DataCleaner:
    """Validates, deduplicates, and tags outliers in market tick records."""

    def __init__(self, rolling_state: RollingState) -> None:
        self._state = rolling_state

    def validate(self, tick: dict) -> tuple[bool, str]:
        """Return (is_valid, reason). Checks price, volume, symbol, and timestamp."""
        if not tick.get("symbol"):
            return False, "empty_symbol"
        if tick.get("price", 0) <= 0:
            return False, "invalid_price"
        if tick.get("volume", -1) < 0:
            return False, "invalid_volume"
        age_ms = abs(int(time.time() * 1000) - tick.get("timestamp_ms", 0))
        if age_ms > _MAX_TICK_AGE_MS:
            return False, "stale_timestamp"
        return True, ""

    def is_duplicate(self, tick: dict) -> bool:
        symbol = tick["symbol"]
        ts_ms = tick["timestamp_ms"]
        price = tick["price"]
        if self._state.is_duplicate(symbol, ts_ms, price):
            return True
        self._state.record_seen(symbol, ts_ms, price)
        # Evict old entries
        cutoff_ms = int(time.time() * 1000) - settings.DEDUP_WINDOW_MS
        self._state.evict_dedup_cache(symbol, cutoff_ms)
        return False

    def tag_outlier(self, tick: dict) -> dict:
        """Add is_outlier flag based on Z-score. Does NOT drop the record."""
        symbol = tick["symbol"]
        price = tick["price"]
        z = self._state.get_zscore(symbol, price)
        is_outlier = z is not None and abs(z) > _ZSCORE_OUTLIER_THRESHOLD
        # Return a copy with the outlier tag
        result = dict(tick)
        result["_is_outlier"] = is_outlier
        result["_zscore"] = z
        return result

    def clean(self, tick: dict) -> dict | None:
        """
        Full cleaning pipeline.
        Returns cleaned tick dict (possibly with outlier tag) or None if rejected.
        """
        valid, reason = self.validate(tick)
        if not valid:
            logger.debug("Tick rejected", reason=reason, symbol=tick.get("symbol"))
            return None

        if self.is_duplicate(tick):
            logger.debug("Tick deduplicated", symbol=tick.get("symbol"), ts_ms=tick.get("timestamp_ms"))
            return None

        # Update rolling state for future Z-score computation
        self._state.update_tick(tick["symbol"], tick["price"], tick["volume"], tick["timestamp_ms"])

        return self.tag_outlier(tick)
