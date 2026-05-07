import math
import time
from typing import NamedTuple

_ALLOWED_FORM_TYPES = {"8-K", "10-K", "10-Q", "S-1", "4", "DEF 14A", "SC 13G", "13F-HR"}
_FUTURE_TOLERANCE_MS = 5_000  # 5 seconds


class ValidationResult(NamedTuple):
    valid: bool
    errors: list[str]


class DataValidator:
    def validate_market_tick(self, record: dict) -> ValidationResult:
        errors: list[str] = []

        symbol = record.get("symbol", "")
        if not symbol or not isinstance(symbol, str):
            errors.append("symbol must be a non-empty string")

        price = record.get("price")
        if price is None or not isinstance(price, (int, float)):
            errors.append("price must be a number")
        elif math.isnan(price) or math.isinf(price):
            errors.append("price must be finite")
        elif price <= 0:
            errors.append("price must be > 0")

        volume = record.get("volume")
        if volume is None or not isinstance(volume, int):
            errors.append("volume must be an integer")
        elif volume < 0:
            errors.append("volume must be >= 0")

        ts = record.get("timestamp_ms")
        if ts is None or not isinstance(ts, (int, float)):
            errors.append("timestamp_ms must be a number")
        elif ts > time.time() * 1000 + _FUTURE_TOLERANCE_MS:
            errors.append("timestamp_ms is too far in the future")

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_news_article(self, record: dict) -> ValidationResult:
        errors: list[str] = []

        headline = record.get("headline", "")
        if not headline or not isinstance(headline, str):
            errors.append("headline must be a non-empty string")

        published_ms = record.get("published_ms")
        if published_ms is None or not isinstance(published_ms, (int, float)):
            errors.append("published_ms must be a number")
        elif published_ms <= 0:
            errors.append("published_ms must be > 0")

        url = record.get("url", "")
        if not url or not isinstance(url, str):
            errors.append("url must be a non-empty string")

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_social_post(self, record: dict) -> ValidationResult:
        errors: list[str] = []

        post_id = record.get("post_id", "")
        if not post_id or not isinstance(post_id, str):
            errors.append("post_id must be a non-empty string")

        title = record.get("title", "")
        if not title or not isinstance(title, str):
            errors.append("title must be a non-empty string")

        created_ms = record.get("created_ms")
        if created_ms is None or not isinstance(created_ms, (int, float)):
            errors.append("created_ms must be a number")
        elif created_ms <= 0:
            errors.append("created_ms must be > 0")

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_sec_filing(self, record: dict) -> ValidationResult:
        errors: list[str] = []

        cik = record.get("cik", "")
        if not cik or not isinstance(cik, str):
            errors.append("cik must be a non-empty string")

        form_type = record.get("form_type", "")
        if form_type not in _ALLOWED_FORM_TYPES:
            errors.append(f"form_type '{form_type}' not in allowed set")

        filed_ms = record.get("filed_ms")
        if filed_ms is None or not isinstance(filed_ms, (int, float)):
            errors.append("filed_ms must be a number")
        elif filed_ms <= 0:
            errors.append("filed_ms must be > 0")

        return ValidationResult(valid=len(errors) == 0, errors=errors)


validator = DataValidator()
