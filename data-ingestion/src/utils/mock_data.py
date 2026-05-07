import random
import time
import uuid

WATCHED_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM"]

_MOCK_PRICES: dict[str, float] = {
    "AAPL": 182.0,
    "MSFT": 375.0,
    "GOOGL": 140.0,
    "AMZN": 178.0,
    "TSLA": 245.0,
    "META": 490.0,
    "NVDA": 870.0,
    "JPM": 195.0,
}

_SUBREDDITS = ["wallstreetbets", "stocks", "investing"]
_FORM_TYPES = ["8-K", "10-K", "10-Q", "S-1", "4"]
_HEADLINES = [
    "{symbol} beats Q3 earnings expectations by 12%",
    "Fed signals rate hold as {symbol} rallies",
    "{symbol} announces $5B share buyback program",
    "Analyst upgrades {symbol} to Strong Buy",
    "{symbol} reports record revenue in latest quarter",
]
_POST_TITLES = [
    "${symbol} to the moon! DD inside",
    "Why I'm bullish on ${symbol} long term",
    "Hot take: ${symbol} is overvalued at these levels",
    "Technical analysis: ${symbol} breaking out",
    "Anyone else loading up on ${symbol} calls?",
]
_COMPANIES = [
    ("0000320193", "Apple Inc.", "AAPL"),
    ("0000789019", "Microsoft Corp", "MSFT"),
    ("0001652044", "Alphabet Inc.", "GOOGL"),
    ("0001018724", "Amazon.com Inc.", "AMZN"),
    ("0001318605", "Tesla Inc.", "TSLA"),
]


class MockDataGenerator:
    """Generates realistic synthetic financial data for development without API keys."""

    def __init__(self, seed: int = 42) -> None:
        self._rng = random.Random(seed)
        self._prices = dict(_MOCK_PRICES)

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _walk_price(self, symbol: str) -> float:
        """Simple random walk with mean reversion."""
        base = self._prices.get(symbol, 100.0)
        pct = self._rng.gauss(0, 0.001)
        new_price = max(1.0, base * (1 + pct))
        self._prices[symbol] = new_price
        return round(new_price, 4)

    def market_tick(self, symbol: str | None = None) -> dict:
        sym = symbol or self._rng.choice(WATCHED_SYMBOLS)
        price = self._walk_price(sym)
        spread = round(price * 0.0002, 4)
        return {
            "event_id": str(uuid.uuid4()),
            "symbol": sym,
            "timestamp_ms": self._now_ms(),
            "price": price,
            "volume": self._rng.randint(100, 10000),
            "bid_price": round(price - spread, 4),
            "ask_price": round(price + spread, 4),
            "bid_size": self._rng.randint(100, 5000),
            "ask_size": self._rng.randint(100, 5000),
            "exchange": self._rng.choice(["NASDAQ", "NYSE", "BATS"]),
            "source": "mock",
            "is_mock": True,
        }

    def market_bar(self, symbol: str | None = None) -> dict:
        sym = symbol or self._rng.choice(WATCHED_SYMBOLS)
        close = self._walk_price(sym)
        high = round(close * (1 + self._rng.uniform(0, 0.005)), 4)
        low = round(close * (1 - self._rng.uniform(0, 0.005)), 4)
        open_price = round(self._rng.uniform(low, high), 4)
        volume = self._rng.randint(10000, 500000)
        return {
            "bar_id": str(uuid.uuid4()),
            "symbol": sym,
            "timeframe": "1min",
            "timestamp_ms": self._now_ms(),
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "vwap": round((open_price + high + low + close) / 4, 4),
            "trade_count": self._rng.randint(50, 5000),
            "source": "mock",
            "is_mock": True,
        }

    def news_article(self) -> dict:
        sym = self._rng.choice(WATCHED_SYMBOLS)
        headline_tpl = self._rng.choice(_HEADLINES)
        headline = headline_tpl.format(symbol=sym)
        now_ms = self._now_ms()
        return {
            "article_id": str(uuid.uuid4()),
            "headline": headline,
            "body": f"Full article body about {sym}. " * 5,
            "url": f"https://mock-news.example.com/{uuid.uuid4()}",
            "source": "MockNews",
            "published_ms": now_ms - self._rng.randint(0, 300_000),
            "ingested_ms": now_ms,
            "symbols": [sym],
            "language": "en",
            "is_mock": True,
        }

    def social_post(self) -> dict:
        sym = self._rng.choice(WATCHED_SYMBOLS)
        title_tpl = self._rng.choice(_POST_TITLES)
        title = title_tpl.format(symbol=sym)
        subreddit = self._rng.choice(_SUBREDDITS)
        post_id = str(uuid.uuid4()).replace("-", "")[:8]
        return {
            "post_id": post_id,
            "platform": "reddit",
            "subreddit": subreddit,
            "title": title,
            "body": f"Mock post about ${sym}. " * 3,
            "author": f"mock_user_{self._rng.randint(1, 9999)}",
            "score": self._rng.randint(0, 5000),
            "num_comments": self._rng.randint(0, 500),
            "created_ms": self._now_ms(),
            "symbols": [sym],
            "url": f"https://reddit.com/r/{subreddit}/comments/{post_id}",
            "is_mock": True,
        }

    def sec_filing(self) -> dict:
        cik, company, ticker = self._rng.choice(_COMPANIES)
        form_type = self._rng.choice(_FORM_TYPES)
        now_ms = self._now_ms()
        accession = f"{cik[:10]}-{uuid.uuid4().hex[:8]}"
        return {
            "filing_id": accession,
            "cik": cik,
            "company_name": company,
            "ticker": ticker,
            "form_type": form_type,
            "filed_ms": now_ms,
            "period_of_report": "2024-12-31",
            "filing_url": f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}.htm",
            "description": f"Mock {form_type} filing for {company}",
            "is_mock": True,
        }


mock_generator = MockDataGenerator()
