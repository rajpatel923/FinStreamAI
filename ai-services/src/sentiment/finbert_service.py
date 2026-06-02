from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)

_LABEL_TO_SCORE: dict[str, float] = {
    "positive": 1.0,
    "neutral": 0.0,
    "negative": -1.0,
}


class FinBERTService:
    """HuggingFace ProsusAI/finbert wrapper for financial text sentiment.

    Model is lazy-loaded on first call (CPU-only, ~440 MB).
    Supports batch processing of up to 32 texts per call.
    """

    def __init__(self) -> None:
        self._pipeline = None

    def _get_pipeline(self):
        if self._pipeline is None:
            from transformers import pipeline as hf_pipeline

            logger.info("Loading FinBERT model (CPU, first-time load)")
            self._pipeline = hf_pipeline(
                "text-classification",
                model="ProsusAI/finbert",
                device=-1,
                batch_size=32,
                truncation=True,
                max_length=512,
            )
            logger.info("FinBERT model loaded")
        return self._pipeline

    def analyze(self, texts: list[str]) -> list[dict]:
        """Classify sentiment for a list of financial texts.

        Returns one result per input:
            {"label": "positive"|"negative"|"neutral", "score": float, "sentiment_score": float}
        where sentiment_score maps positive→1.0, neutral→0.0, negative→-1.0.
        """
        if not texts:
            return []
        pipe = self._get_pipeline()
        raw = pipe(texts)
        return [
            {
                "label": r["label"].lower(),
                "score": round(r["score"], 4),
                "sentiment_score": _LABEL_TO_SCORE.get(r["label"].lower(), 0.0),
            }
            for r in raw
        ]

    def analyze_one(self, text: str) -> dict:
        return self.analyze([text])[0]
