import re
import time
from collections import OrderedDict
from typing import Any

import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.mock_data import WATCHED_SYMBOLS, mock_generator
from src.utils.monitoring import mock_messages_total, validation_failures_total

logger = structlog.get_logger(__name__)

_CASHTAG_RE = re.compile(r"\$([A-Z]{1,5})")
_DEDUP_MAX = 50_000


class RedditProducer(BaseProducer):
    """Produces Reddit posts from financial subreddits to social.posts.raw."""

    def __init__(self) -> None:
        super().__init__("reddit_producer", "social.posts.raw")
        cfg = data_source_config
        self._client_id = cfg.REDDIT_CLIENT_ID
        self._client_secret = cfg.REDDIT_CLIENT_SECRET
        self._user_agent = cfg.REDDIT_USER_AGENT
        self._use_mock = cfg.USE_MOCK_DATA or not self._client_id
        self._subreddits = cfg.reddit_subreddits_list
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._praw = None

        source = "mock" if self._use_mock else "reddit"
        logger.info("RedditProducer initialised", source=source, subreddits=self._subreddits)

    def _get_praw(self) -> Any:
        if self._praw is None:
            import praw
            self._praw = praw.Reddit(
                client_id=self._client_id,
                client_secret=self._client_secret,
                user_agent=self._user_agent,
            )
        return self._praw

    def data_source_name(self) -> str:
        return "mock" if self._use_mock else "reddit"

    def poll_interval_seconds(self) -> float:
        return 60.0

    def fetch_data(self) -> list[dict]:
        if self._use_mock:
            return [mock_generator.social_post() for _ in range(10)]
        return self._fetch_reddit()

    def _fetch_reddit(self) -> list[dict]:
        posts: list[dict] = []
        reddit = self._get_praw()
        for sub_name in self._subreddits:
            try:
                subreddit = reddit.subreddit(sub_name)
                for post in subreddit.new(limit=25):
                    if post.id in self._seen:
                        continue
                    body = post.selftext or ""
                    symbols = self._extract_symbols(post.title + " " + body)
                    posts.append({
                        "post_id": post.id,
                        "platform": "reddit",
                        "subreddit": sub_name,
                        "title": post.title,
                        "body": body if body else None,
                        "author": str(post.author) if post.author else None,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "created_ms": int(post.created_utc * 1000),
                        "symbols": symbols,
                        "url": f"https://reddit.com{post.permalink}",
                        "is_mock": False,
                    })
            except Exception as exc:
                logger.warning("Reddit fetch failed", subreddit=sub_name, error=str(exc))
        return posts

    def _extract_symbols(self, text: str) -> list[str]:
        found = _CASHTAG_RE.findall(text)
        return list(dict.fromkeys(s for s in found if s in WATCHED_SYMBOLS))

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        now = time.time()
        for record in raw_data:
            post_id = record.get("post_id", "")
            if post_id and post_id in self._seen:
                continue

            vr = validator.validate_social_post(record)
            if not vr.valid:
                for err in vr.errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid social post", errors=vr.errors)
                continue

            # LRU eviction at max size
            if len(self._seen) >= _DEDUP_MAX:
                self._seen.popitem(last=False)

            if post_id:
                self._seen[post_id] = now

            payload = avro_serializer.serialize("social_post", record)
            if record.get("is_mock"):
                mock_messages_total.labels(producer=self.producer_name).inc()
            key = record["symbols"][0] if record.get("symbols") else None
            results.append((key, payload))
        return results
