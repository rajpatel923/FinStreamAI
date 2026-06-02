from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


class NERService:
    """dslim/bert-base-NER wrapper for extracting financial entities.

    Extracts ORG (companies), PER (persons), LOC (locations) from text.
    Lazy-loaded on first call (CPU-only).
    """

    def __init__(self) -> None:
        self._pipeline = None

    def _get_pipeline(self):
        if self._pipeline is None:
            from transformers import pipeline as hf_pipeline

            logger.info("Loading BERT-NER model (CPU, first-time load)")
            self._pipeline = hf_pipeline(
                "ner",
                model="dslim/bert-base-NER",
                aggregation_strategy="simple",
                device=-1,
            )
            logger.info("BERT-NER model loaded")
        return self._pipeline

    def extract_entities(self, text: str) -> dict:
        """Extract named entities from financial text.

        Returns:
            {"companies": [...], "persons": [...], "locations": [...]}
        """
        pipe = self._get_pipeline()
        entities = pipe(text[:512])
        companies = list({e["word"] for e in entities if e["entity_group"] == "ORG"})
        persons = list({e["word"] for e in entities if e["entity_group"] == "PER"})
        locations = list({e["word"] for e in entities if e["entity_group"] == "LOC"})
        return {"companies": companies, "persons": persons, "locations": locations}
