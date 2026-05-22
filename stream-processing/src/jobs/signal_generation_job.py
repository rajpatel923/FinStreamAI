import json
import time
import uuid
from typing import Any

import structlog

from src.config import settings
from src.jobs.base_job import BaseJob
from src.sinks.timescaledb_sink import TimescaleDBSink
from src.utils.monitoring import signals_generated_total

logger = structlog.get_logger(__name__)

_TOPIC_INPUT = "technical.indicators"
_TOPIC_OUTPUT = "predictions.signals"


class SignalGenerationJob(BaseJob):
    """
    Consumes technical.indicators, applies rule-based signal generation,
    produces to predictions.signals and writes to TimescaleDB trading_signals.
    """

    def __init__(self, db_sink: TimescaleDBSink | None = None) -> None:
        super().__init__(
            job_name="signals",
            input_topics=[_TOPIC_INPUT],
            output_topics=[_TOPIC_OUTPUT],
            input_schema="technical_indicator",
        )
        # latest indicator values per symbol: {symbol: {indicator_name: value}}
        self._latest: dict[str, dict[str, float]] = {}
        # extra fields per indicator (bands, signal line)
        self._latest_meta: dict[str, dict[str, dict]] = {}
        # dedup: {symbol: {signal_type: last_emitted_ms}}
        self._signal_dedup: dict[str, dict[str, int]] = {}
        self._db_sink = db_sink or TimescaleDBSink()

    def process(self, record: dict, raw_msg: Any) -> list[tuple[str, bytes, bytes]]:
        symbol = record["symbol"]
        ind_name = record["indicator_name"]
        value = record["value"]
        ts_ms = record["timestamp_ms"]

        # Update latest state
        if symbol not in self._latest:
            self._latest[symbol] = {}
            self._latest_meta[symbol] = {}
        self._latest[symbol][ind_name] = value
        self._latest_meta[symbol][ind_name] = {
            "signal_value": record.get("signal_value"),
            "upper_band": record.get("upper_band"),
            "lower_band": record.get("lower_band"),
        }

        signals = self._generate_signals(symbol, ts_ms)
        outputs = []
        for sig in signals:
            key_bytes = sig["symbol"].encode()
            value_bytes = json.dumps(sig).encode()
            outputs.append((_TOPIC_OUTPUT, key_bytes, value_bytes))
            signals_generated_total.labels(
                direction=sig["direction"],
                signal_type=sig["signal_type"],
            ).inc()
            try:
                self._db_sink.write_trading_signal(sig)
            except Exception as exc:
                logger.error("TimescaleDB signal write failed", job=self.job_name, error=str(exc))

        return outputs

    def _is_deduped(self, symbol: str, signal_type: str, ts_ms: int) -> bool:
        dedup = self._signal_dedup.setdefault(symbol, {})
        last_ms = dedup.get(signal_type, 0)
        window_ms = settings.SIGNAL_DEDUP_WINDOW_S * 1000
        if ts_ms - last_ms < window_ms:
            return True
        dedup[signal_type] = ts_ms
        return False

    def _generate_signals(self, symbol: str, ts_ms: int) -> list[dict]:
        indicators = self._latest.get(symbol, {})
        meta = self._latest_meta.get(symbol, {})
        signals = []

        # ── RSI signals ────────────────────────────────────────
        rsi = indicators.get("RSI14")
        if rsi is not None:
            if rsi < settings.RSI_OVERSOLD:
                strength = (settings.RSI_OVERSOLD - rsi) / settings.RSI_OVERSOLD
                sig = self._make_signal(symbol, ts_ms, "RSI", "BUY", min(strength, 1.0), indicators)
                if sig and not self._is_deduped(symbol, "RSI_BUY", ts_ms):
                    signals.append(sig)
            elif rsi > settings.RSI_OVERBOUGHT:
                strength = (rsi - settings.RSI_OVERBOUGHT) / (100.0 - settings.RSI_OVERBOUGHT)
                sig = self._make_signal(symbol, ts_ms, "RSI", "SELL", min(strength, 1.0), indicators)
                if sig and not self._is_deduped(symbol, "RSI_SELL", ts_ms):
                    signals.append(sig)

        # ── MACD crossover ─────────────────────────────────────
        macd_val = indicators.get("MACD")
        macd_signal = meta.get("MACD", {}).get("signal_value")
        if macd_val is not None and macd_signal is not None:
            histogram = macd_val - macd_signal
            prev_hist = getattr(self, "_prev_macd_hist", {}).get(symbol)
            if not hasattr(self, "_prev_macd_hist"):
                self._prev_macd_hist: dict[str, float] = {}
            if prev_hist is not None:
                if prev_hist < 0 and histogram >= 0:
                    sig = self._make_signal(symbol, ts_ms, "MACD_CROSSOVER", "BUY", 0.6, indicators)
                    if sig and not self._is_deduped(symbol, "MACD_BUY", ts_ms):
                        signals.append(sig)
                elif prev_hist >= 0 and histogram < 0:
                    sig = self._make_signal(symbol, ts_ms, "MACD_CROSSOVER", "SELL", 0.6, indicators)
                    if sig and not self._is_deduped(symbol, "MACD_SELL", ts_ms):
                        signals.append(sig)
            self._prev_macd_hist[symbol] = histogram

        # ── Bollinger Band signals ─────────────────────────────
        bb_upper = meta.get("BB20", {}).get("upper_band")
        bb_lower = meta.get("BB20", {}).get("lower_band")
        bb_mid = indicators.get("BB20")
        # Use SMA20 as price proxy if we have it, or skip
        sma20 = indicators.get("SMA20")
        if bb_upper is not None and bb_lower is not None and sma20 is not None:
            if sma20 < bb_lower:
                sig = self._make_signal(symbol, ts_ms, "BB_BAND", "BUY", 0.5, indicators)
                if sig and not self._is_deduped(symbol, "BB_BUY", ts_ms):
                    signals.append(sig)
            elif sma20 > bb_upper:
                sig = self._make_signal(symbol, ts_ms, "BB_BAND", "SELL", 0.5, indicators)
                if sig and not self._is_deduped(symbol, "BB_SELL", ts_ms):
                    signals.append(sig)

        return signals

    @staticmethod
    def _make_signal(
        symbol: str,
        ts_ms: int,
        signal_type: str,
        direction: str,
        strength: float,
        indicators: dict,
    ) -> dict | None:
        return {
            "signal_id": str(uuid.uuid4()),
            "symbol": symbol,
            "timestamp_ms": ts_ms,
            "signal_type": signal_type,
            "direction": direction,
            "strength": min(strength, 1.0),
            "indicator_values": {k: str(v) for k, v in indicators.items()},
            "source": "signal-generation",
        }

    def run(self) -> None:
        try:
            super().run()
        finally:
            self._db_sink.flush()
