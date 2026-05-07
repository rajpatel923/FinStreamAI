import re
import time
import uuid
from collections import OrderedDict
from typing import Any

import httpx
import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.mock_data import WATCHED_SYMBOLS, mock_generator
from src.utils.monitoring import mock_messages_total, validation_failures_total

logger = structlog.get_logger(__name__)

_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})")
_DEDUP_MAX = 10_000
_DEDUP_TTL_SECONDS = 3600


class NewsProducer(BaseProducer):
    """Produces news articles to news.articles.raw."""

    def __init__(self) -> None:
        super().__init__("news_producer", "news.articles.raw")
        cfg = data_source_config
        self._api_key = cfg.NEWS_API_KEY
        self._use_mock = cfg.USE_MOCK_DATA or not self._api_key
        self._keywords = cfg.NEWS_FINANCIAL_KEYWORDS
        self._http = httpx.Client(timeout=cfg.HTTP_TIMEOUT_SECONDS)
        # Dedup: url -> ingested_ms
        self._seen: OrderedDict[str, float] = OrderedDict()

        source = "mock" if self._use_mock else "newsapi"
        logger.info("NewsProducer initialised", source=source)

    def data_source_name(self) -> str:
        return "mock" if self._use_mock else "newsapi"

    def poll_interval_seconds(self) -> float:
        return 300.0

    def fetch_data(self) -> list[dict]:
        self._evict_expired()
        if self._use_mock:
            return [mock_generator.news_article() for _ in range(5)]
        return self._fetch_newsapi()

    def _evict_expired(self) -> None:
        cutoff = time.time() - _DEDUP_TTL_SECONDS
        expired = [url for url, ts in self._seen.items() if ts < cutoff]
        for url in expired:
            del self._seen[url]

    def _fetch_newsapi(self) -> list[dict]:
        articles: list[dict] = []
        try:
            resp = self._http.get(
                data_source_config.NEWS_API_BASE_URL,
                params={
                    "q": self._keywords,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 20,
                    "apiKey": self._api_key,
                },
            )
            resp.raise_for_status()
            for art in resp.json().get("articles", []):
                url = art.get("url", "")
                if not url or url in self._seen:
                    continue
                now_ms = int(time.time() * 1000)
                published_ms = self._parse_published(art.get("publishedAt", ""))
                headline = art.get("title", "")
                symbols = self._extract_symbols(headline + " " + (art.get("description") or ""))
                articles.append({
                    "article_id": str(uuid.uuid4()),
                    "headline": headline,
                    "body": art.get("content"),
                    "url": url,
                    "source": art.get("source", {}).get("name", "unknown"),
                    "published_ms": published_ms,
                    "ingested_ms": now_ms,
                    "symbols": symbols,
                    "language": "en",
                    "is_mock": False,
                })
        except Exception as exc:
            logger.warning("NewsAPI fetch failed", error=str(exc))
        return articles

    def _parse_published(self, iso: str) -> int:
        if not iso:
            return int(time.time() * 1000)
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(time.time() * 1000)

    def _extract_symbols(self, text: str) -> list[str]:
        found = _CASHTAG_RE.findall(text)
        return list(dict.fromkeys(s for s in found if s in WATCHED_SYMBOLS))

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        now = time.time()
        for record in raw_data:
            url = record.get("url", "")
            if url and url in self._seen:
                continue

            vr = validator.validate_news_article(record)
            if not vr.valid:
                for err in vr.errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid news article", errors=vr.errors)
                continue

            # Enforce dedup max size (LRU eviction)
            if len(self._seen) >= _DEDUP_MAX:
                self._seen.popitem(last=False)

            if url:
                self._seen[url] = now

            payload = avro_serializer.serialize("news_article", record)
            if record.get("is_mock"):
                mock_messages_total.labels(producer=self.producer_name).inc()
            key = record["symbols"][0] if record.get("symbols") else None
            results.append((key, payload))
        return results
