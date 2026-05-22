import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from threading import Lock

from src.config import settings


@dataclass
class SymbolState:
    """Per-symbol incremental state for indicator computation."""

    prices: deque = field(default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN))
    volumes: deque = field(default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN))
    closes: deque = field(default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN))
    highs: deque = field(default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN))
    lows: deque = field(default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN))
    prev_close: float | None = None
    bar_count: int = 0
    ema_states: dict = field(default_factory=dict)  # e.g. {"EMA12": 0.0, "EMA26": 0.0, "MACD_signal": 0.0}
    macd_history: deque = field(default_factory=lambda: deque(maxlen=50))
    rsi_gains: deque = field(default_factory=lambda: deque(maxlen=50))
    rsi_losses: deque = field(default_factory=lambda: deque(maxlen=50))
    daily_vwap_date: str | None = None
    daily_pv_sum: float = 0.0
    daily_vol_sum: float = 0.0
    # dedup_cache: (ts_ms, price_str) -> wall_clock_s
    dedup_cache: OrderedDict = field(default_factory=OrderedDict)
    # momentum: ts_ms -> price (ring for lookback)
    price_history_ts: deque = field(
        default_factory=lambda: deque(maxlen=settings.INDICATOR_DEQUE_MAXLEN)
    )  # list of (ts_ms, price) tuples


class RollingState:
    """Thread-safe per-symbol rolling state registry."""

    def __init__(self) -> None:
        self._states: dict[str, SymbolState] = {}
        self._lock = Lock()

    def get_or_create(self, symbol: str) -> SymbolState:
        if symbol not in self._states:
            with self._lock:
                if symbol not in self._states:
                    self._states[symbol] = SymbolState()
        return self._states[symbol]

    def update_tick(self, symbol: str, price: float, volume: int, ts_ms: int) -> SymbolState:
        state = self.get_or_create(symbol)
        state.prices.append(price)
        state.volumes.append(volume)
        state.price_history_ts.append((ts_ms, price))
        return state

    def update_bar(self, symbol: str, bar: dict) -> SymbolState:
        """Update state from a OHLCV bar record."""
        state = self.get_or_create(symbol)
        close = bar["close"]
        high = bar["high"]
        low = bar["low"]
        volume = bar["volume"]

        # RSI gains/losses
        if state.prev_close is not None:
            delta = close - state.prev_close
            state.rsi_gains.append(max(delta, 0.0))
            state.rsi_losses.append(max(-delta, 0.0))

        state.prev_close = close
        state.closes.append(close)
        state.highs.append(high)
        state.lows.append(low)
        state.volumes.append(int(volume))
        state.bar_count += 1
        return state

    def get_zscore(self, symbol: str, value: float, window: int = 30) -> float | None:
        state = self.get_or_create(symbol)
        prices = list(state.prices)
        if len(prices) < window:
            return None
        recent = prices[-window:]
        mean = sum(recent) / window
        variance = sum((p - mean) ** 2 for p in recent) / window
        std = variance**0.5
        if std == 0.0:
            return None
        return (value - mean) / std

    def get_volume_ratio(self, symbol: str, volume: int, window: int = 20) -> float | None:
        state = self.get_or_create(symbol)
        vols = list(state.volumes)
        if len(vols) < window:
            return None
        avg = sum(vols[-window:]) / window
        if avg == 0:
            return None
        return volume / avg

    def get_price_change_pct(
        self, symbol: str, current_price: float, lookback_ms: int = 60_000
    ) -> float | None:
        """Return % price change vs the oldest tick within lookback_ms."""
        state = self.get_or_create(symbol)
        if not state.price_history_ts:
            return None
        cutoff_ms = int(time.time() * 1000) - lookback_ms
        past_price = None
        for ts_ms, price in state.price_history_ts:
            if ts_ms <= cutoff_ms:
                past_price = price
            else:
                break
        if past_price is None or past_price == 0:
            return None
        return abs(current_price - past_price) / past_price * 100.0

    def is_duplicate(self, symbol: str, ts_ms: int, price: float) -> bool:
        state = self.get_or_create(symbol)
        key = (ts_ms, f"{price:.6f}")
        return key in state.dedup_cache

    def record_seen(self, symbol: str, ts_ms: int, price: float) -> None:
        state = self.get_or_create(symbol)
        key = (ts_ms, f"{price:.6f}")
        state.dedup_cache[key] = time.time()

    def evict_dedup_cache(self, symbol: str, cutoff_ms: int) -> None:
        state = self.get_or_create(symbol)
        cutoff_s = cutoff_ms / 1000.0
        keys_to_delete = [k for k, wall_s in state.dedup_cache.items() if wall_s < cutoff_s]
        for k in keys_to_delete:
            del state.dedup_cache[k]
