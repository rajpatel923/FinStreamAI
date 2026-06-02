from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/sentiment", tags=["sentiment"])


class AnalyzeRequest(BaseModel):
    text: str | None = None
    texts: list[str] | None = None


class SentimentResult(BaseModel):
    label: str
    score: float
    sentiment_score: float


@router.post("/analyze")
def analyze(req: AnalyzeRequest, request: Request) -> dict:
    """Run FinBERT sentiment on one text or a list of texts."""
    finbert = request.app.state.finbert
    if req.texts:
        results = finbert.analyze(req.texts)
        return {"results": results, "count": len(results)}
    if req.text:
        result = finbert.analyze_one(req.text)
        return result
    return {"error": "provide 'text' or 'texts'"}


@router.post("/entities")
def extract_entities(req: AnalyzeRequest, request: Request) -> dict:
    """Run BERT-NER entity extraction on a single text."""
    ner = request.app.state.ner
    if not req.text:
        return {"error": "provide 'text'"}
    return ner.extract_entities(req.text)
