import signal
import threading
import time

import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.producers.market_bar_producer import MarketBarProducer
from src.producers.market_data_producer import MarketDataProducer
from src.producers.news_producer import NewsProducer
from src.producers.sec_filing_producer import SECFilingProducer
from src.producers.social_media_producer import RedditProducer
from src.utils.monitoring import start_metrics_server

logger = structlog.get_logger(__name__)

shutdown_event = threading.Event()


def _handle_shutdown(signum: int, frame: object) -> None:
    logger.info("Shutdown signal received, stopping all producers", signal=signum)
    shutdown_event.set()


def build_producers() -> list[BaseProducer]:
    return [
        MarketDataProducer(),
        MarketBarProducer(),
        NewsProducer(),
        RedditProducer(),
        SECFilingProducer(),
    ]


def run_producer_thread(producer: BaseProducer) -> None:
    """Run producer.run() in a loop, restarting on unexpected crashes."""
    while not shutdown_event.is_set():
        try:
            producer._running = True
            producer.run()
        except Exception as exc:
            logger.error(
                "Producer crashed, restarting in 30s",
                producer=producer.producer_name,
                error=str(exc),
            )
            if not shutdown_event.is_set():
                shutdown_event.wait(timeout=30)

    # Signal the producer's own loop to stop
    producer._running = False
    logger.info("Producer thread exiting", producer=producer.producer_name)


def main() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    start_metrics_server(port=data_source_config.METRICS_PORT)
    logger.info("Metrics server started", port=data_source_config.METRICS_PORT)

    producers = build_producers()
    threads = [
        threading.Thread(
            target=run_producer_thread,
            args=(p,),
            name=p.producer_name,
            daemon=True,
        )
        for p in producers
    ]

    for t in threads:
        t.start()
        logger.info("Producer thread started", name=t.name)

    # Wait until shutdown signal
    shutdown_event.wait()

    logger.info("Waiting for producer threads to finish")
    for t in threads:
        t.join(timeout=35)

    logger.info("All producers stopped. Goodbye.")


if __name__ == "__main__":
    main()
