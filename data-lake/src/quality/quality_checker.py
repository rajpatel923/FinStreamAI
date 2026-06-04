"""Data quality validation rules per record type."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class QualityResult:
    passed: bool
    violations: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.passed


class QualityChecker:
    """Validates records against type-specific rules."""

    # ------------------------------------------------------------------
    # Rule registry
    # ------------------------------------------------------------------
    def check(self, record_type: str, record: dict[str, Any]) -> QualityResult:
        method = getattr(self, f"_check_{record_type}", None)
        if method is None:
            return QualityResult(passed=True)
        violations: list[str] = method(record)
        return QualityResult(passed=len(violations) == 0, violations=violations)

    @staticmethod
    def score(results: list[QualityResult]) -> float:
        """Return fraction of results that passed (0.0 – 1.0)."""
        if not results:
            return 1.0
        return sum(1 for r in results if r.passed) / len(results)

    # ------------------------------------------------------------------
    # Per-type rules
    # ------------------------------------------------------------------
    def _check_market_tick(self, r: dict[str, Any]) -> list[str]:
        v: list[str] = []
        for f in ("symbol", "price", "timestamp"):
            if r.get(f) is None:
                v.append(f"missing required field: {f}")
        if r.get("price") is not None:
            try:
                p = float(r["price"])
                if p <= 0:
                    v.append(f"price must be > 0, got {p}")
            except (TypeError, ValueError):
                v.append(f"price is not numeric: {r['price']}")
        if r.get("volume") is not None:
            try:
                vol = float(r["volume"])
                if vol < 0:
                    v.append(f"volume must be >= 0, got {vol}")
            except (TypeError, ValueError):
                v.append(f"volume is not numeric: {r['volume']}")
        return v

    def _check_news_article(self, r: dict[str, Any]) -> list[str]:
        v: list[str] = []
        for f in ("article_id", "title", "timestamp"):
            if not r.get(f):
                v.append(f"missing required field: {f}")
        if r.get("sentiment_score") is not None:
            try:
                s = float(r["sentiment_score"])
                if not -1.0 <= s <= 1.0:
                    v.append(f"sentiment_score {s} not in [-1, 1]")
            except (TypeError, ValueError):
                v.append(f"sentiment_score not numeric: {r['sentiment_score']}")
        return v

    def _check_social_post(self, r: dict[str, Any]) -> list[str]:
        v: list[str] = []
        for f in ("post_id", "content", "timestamp"):
            if not r.get(f):
                v.append(f"missing required field: {f}")
        if r.get("sentiment_score") is not None:
            try:
                s = float(r["sentiment_score"])
                if not -1.0 <= s <= 1.0:
                    v.append(f"sentiment_score {s} not in [-1, 1]")
            except (TypeError, ValueError):
                v.append(f"sentiment_score not numeric: {r['sentiment_score']}")
        return v

    def _check_event(self, r: dict[str, Any]) -> list[str]:
        v: list[str] = []
        for f in ("event_id", "event_type", "timestamp"):
            if not r.get(f):
                v.append(f"missing required field: {f}")
        return v
