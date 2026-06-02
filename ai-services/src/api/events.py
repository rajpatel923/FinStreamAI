from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(prefix="/events", tags=["events"])


class ExtractRequest(BaseModel):
    text: str
    source_id: str | None = None


@router.post("/extract")
def extract(req: ExtractRequest, request: Request) -> dict:
    """Extract structured financial events from text using Claude API."""
    extractor = request.app.state.extractor
    return extractor.extract(req.text, source_id=req.source_id)
