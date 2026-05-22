import uuid
from dataclasses import dataclass, field
from threading import Lock

from src.config import settings

TIMEFRAME_MS: dict[str, int] = {
    "5min": 300_000,
    "15min": 900_000,
    "1hour": 3_600_000,
}


@dataclass
class WindowBuffer:
    symbol: str
    timeframe: str
    window_start_ms: int
    window_end_ms: int
    open_price: float | None = None
    high_price: float = float("-inf")
    low_price: float = float("inf")
    close_price: float | None = None
    close_timestamp_ms: int = 0
    volume_sum: int = 0
    pv_sum: float = 0.0
    trade_count: int = 0


# Key: (symbol, timeframe, window_start_ms)
_WindowKey = tuple[str, str, int]


class WindowState:
    """Event-time windowed buffers for OHLCV aggregation across multiple timeframes."""

    def __init__(self) -> None:
        self._buffers: dict[_WindowKey, WindowBuffer] = {}
        self._lock = Lock()

    @staticmethod
    def assign_window(ts_ms: int, timeframe: str) -> tuple[int, int]:
        tf_ms = TIMEFRAME_MS[timeframe]
        window_start = (ts_ms // tf_ms) * tf_ms
        window_end = window_start + tf_ms
        return window_start, window_end

    def update(self, symbol: str, tick: dict) -> None:
        ts_ms: int = tick["timestamp_ms"]
        price: float = tick["price"]
        volume: int = tick["volume"]

        with self._lock:
            for timeframe in TIMEFRAME_MS:
                window_start, window_end = self.assign_window(ts_ms, timeframe)
                key = (symbol, timeframe, window_start)

                if key not in self._buffers:
                    self._buffers[key] = WindowBuffer(
                        symbol=symbol,
                        timeframe=timeframe,
                        window_start_ms=window_start,
                        window_end_ms=window_end,
                    )

                buf = self._buffers[key]
                if buf.open_price is None:
                    buf.open_price = price
                buf.high_price = max(buf.high_price, price)
                buf.low_price = min(buf.low_price, price)
                buf.close_price = price
                buf.close_timestamp_ms = ts_ms
                buf.volume_sum += volume
                buf.pv_sum += price * volume
                buf.trade_count += 1

    def get_closeable_windows(self, now_ms: int) -> list[WindowBuffer]:
        """Return windows whose end + grace period has passed."""
        grace = settings.WINDOW_GRACE_PERIOD_MS
        closeable = []
        with self._lock:
            for buf in self._buffers.values():
                if now_ms > buf.window_end_ms + grace and buf.open_price is not None:
                    closeable.append(buf)
        return closeable

    def remove_window(self, symbol: str, timeframe: str, window_start_ms: int) -> None:
        key = (symbol, timeframe, window_start_ms)
        with self._lock:
            self._buffers.pop(key, None)

    def active_count(self) -> int:
        return len(self._buffers)

    @staticmethod
    def build_bar_record(buf: WindowBuffer) -> dict:
        vwap = buf.pv_sum / buf.volume_sum if buf.volume_sum > 0 else None
        return {
            "bar_id": str(uuid.uuid4()),
            "symbol": buf.symbol,
            "timeframe": buf.timeframe,
            "timestamp_ms": buf.window_start_ms,
            "open": buf.open_price,
            "high": buf.high_price,
            "low": buf.low_price,
            "close": buf.close_price,
            "volume": buf.volume_sum,
            "vwap": vwap,
            "trade_count": buf.trade_count,
            "source": "stream-aggregation",
            "is_mock": False,
        }
