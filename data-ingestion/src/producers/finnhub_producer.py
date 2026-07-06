import json
import queue
import threading
import time
import uuid
from typing import Any

import structlog
import websocket

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.monitoring import validation_failures_total

logger = structlog.get_logger(__name__)

_WS_RECONNECT_DELAY = 5.0
_QUEUE_MAX = 10_000


class FinnhubProducer(BaseProducer):
    """Real-time market tick producer via Finnhub WebSocket feed.

    The WebSocket receiver runs in a daemon thread and enqueues raw trade
    events.  The BaseProducer poll loop drains the queue, validates, and
    serialises each tick to market.ticks.raw.
    """

    def __init__(self) -> None:
        super().__init__("finnhub_producer", "market.ticks.raw")
        cfg = data_source_config
        self._api_key = cfg.FINNHUB_API_KEY
        self._ws_url = cfg.FINNHUB_WS_URL
        self._symbols = cfg.watched_symbols_list
        self._tick_queue: queue.Queue[dict] = queue.Queue(maxsize=_QUEUE_MAX)
        self._ws_thread: threading.Thread | None = None

        logger.info(
            "FinnhubProducer initialised",
            symbols=self._symbols,
            ws_url=self._ws_url,
        )

    # ─── BaseProducer interface ──────────────────────────────────────────────

    def data_source_name(self) -> str:
        return "finnhub_ws"

    def poll_interval_seconds(self) -> float:
        return 0.05  # drain queue rapidly

    def fetch_data(self) -> list[dict]:
        items: list[dict] = []
        try:
            while True:
                items.append(self._tick_queue.get_nowait())
        except queue.Empty:
            pass
        return items

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        for record in raw_data:
            vr = validator.validate_market_tick(record)
            if not vr.valid:
                for err in vr.errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid Finnhub tick", errors=vr.errors)
                continue
            payload = avro_serializer.serialize("market_tick", record)
            results.append((record["symbol"], payload))
        return results

    def run(self) -> None:
        self._running = True
        self._ws_thread = threading.Thread(
            target=self._ws_loop,
            daemon=True,
            name="finnhub_ws",
        )
        self._ws_thread.start()
        logger.info("Finnhub WebSocket thread started", producer=self.producer_name)
        super().run()

    # ─── WebSocket internals ─────────────────────────────────────────────────

    def _ws_loop(self) -> None:
        """Reconnect loop — runs in a daemon thread."""
        while self._running:
            try:
                ws = websocket.WebSocketApp(
                    f"{self._ws_url}?token={self._api_key}",
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error("WebSocket run_forever raised", error=str(exc))
            if self._running:
                logger.info(
                    "WebSocket disconnected, reconnecting",
                    delay=_WS_RECONNECT_DELAY,
                    producer=self.producer_name,
                )
                time.sleep(_WS_RECONNECT_DELAY)

    def _on_open(self, ws: Any) -> None:
        logger.info("Finnhub WebSocket connected", producer=self.producer_name)
        for sym in self._symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        logger.info("Subscribed to symbols", symbols=self._symbols)

    def _on_message(self, ws: Any, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        if msg.get("type") != "trade":
            return

        for trade in msg.get("data", []):
            symbol = trade.get("s", "")
            if not symbol:
                continue
            tick = {
                "event_id": str(uuid.uuid4()),
                "symbol": symbol,
                "timestamp_ms": int(trade.get("t", int(time.time() * 1000))),
                "price": float(trade.get("p", 0.0)),
                "volume": int(trade.get("v", 0)),
                "bid_price": None,
                "ask_price": None,
                "bid_size": None,
                "ask_size": None,
                "exchange": None,
                "source": "finnhub",
                "is_mock": False,
            }
            try:
                self._tick_queue.put_nowait(tick)
            except queue.Full:
                logger.warning(
                    "Finnhub tick queue full, dropping trade",
                    symbol=symbol,
                    producer=self.producer_name,
                )

    def _on_error(self, ws: Any, error: Any) -> None:
        logger.error("Finnhub WebSocket error", error=str(error), producer=self.producer_name)

    def _on_close(self, ws: Any, close_status_code: Any, close_msg: Any) -> None:
        logger.info(
            "Finnhub WebSocket closed",
            status=close_status_code,
            msg=close_msg,
            producer=self.producer_name,
        )
