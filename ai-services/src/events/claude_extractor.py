from __future__ import annotations

import json
import time

import structlog

from src.config import settings

logger = structlog.get_logger(__name__)

_EVENT_TYPES = [
    "merger_acquisition",
    "earnings",
    "executive_change",
    "product_launch",
    "regulatory",
    "dividend",
    "other",
]

_PROMPT_TEMPLATE = """\
Extract financial events from the text below.
Return ONLY a valid JSON object with these exact keys:
- event_type: one of {event_types}
- companies: list of company ticker symbols or names mentioned
- date: ISO date string if a specific date is mentioned, else null
- confidence: float 0.0-1.0 indicating extraction confidence
- summary: one-sentence description of the event

Text:
{text}"""


class ClaudeEventExtractor:
    """Uses claude-sonnet-4-6 to extract structured financial events from text.

    Falls back to spaCy entity extraction if Claude API call fails
    (e.g., quota exceeded or key missing).
    """

    def __init__(self) -> None:
        self._client = None
        self._spacy_nlp = None

    def _get_client(self):
        if self._client is None:
            import anthropic

            self._client = anthropic.Anthropic(
                api_key=settings.ANTHROPIC_API_KEY or None,
            )
        return self._client

    def _call_llm(self, prompt: str) -> str:
        """Dispatch to whichever provider LLM_PROVIDER selects.

        llama_cpp/openrouter both speak the OpenAI chat-completions wire
        format (llama-server's own API), so one httpx call covers both.
        """
        provider = settings.LLM_PROVIDER.lower()
        if provider == "llama_cpp":
            base_url, model, api_key = (
                settings.LLAMA_CPP_BASE_URL,
                settings.LLAMA_CPP_MODEL,
                settings.LLAMA_CPP_API_KEY,
            )
        elif provider == "openrouter":
            base_url, model, api_key = (
                settings.OPENROUTER_BASE_URL,
                settings.OPENROUTER_MODEL,
                settings.OPENROUTER_API_KEY,
            )
        else:
            client = self._get_client()
            response = client.messages.create(
                model=settings.ANTHROPIC_MODEL,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text

        import httpx

        response = httpx.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 512,
            },
            timeout=120,
        )
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def _get_spacy(self):
        if self._spacy_nlp is None:
            import spacy

            try:
                self._spacy_nlp = spacy.load("en_core_web_sm")
            except OSError:
                logger.warning("spaCy en_core_web_sm not found — fallback NER disabled")
                self._spacy_nlp = False
        return self._spacy_nlp

    def extract(self, text: str, source_id: str | None = None) -> dict:
        """Extract financial event from text.

        Returns a dict with: event_type, companies, date, confidence, summary,
        source_id, extracted_ms. Adds "error" key if extraction failed.
        """
        base = {"source_id": source_id, "extracted_ms": int(time.time() * 1000)}
        try:
            prompt = _PROMPT_TEMPLATE.format(
                event_types=_EVENT_TYPES,
                text=text[:4000],
            )
            raw = self._call_llm(prompt).strip()
            # Strip markdown code fences if Claude wraps the JSON
            if raw.startswith("```"):
                parts = raw.split("```")
                raw = parts[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            result = json.loads(raw.strip())
            result.update(base)
            return result
        except Exception as exc:
            logger.warning(
                "Claude extraction failed, falling back to spaCy",
                source_id=source_id,
                error=str(exc),
            )
            return self._spacy_fallback(text, base, str(exc))

    def _spacy_fallback(self, text: str, base: dict, error: str) -> dict:
        nlp = self._get_spacy()
        companies: list[str] = []
        if nlp:
            doc = nlp(text[:1000])
            companies = list({ent.text for ent in doc.ents if ent.label_ == "ORG"})
        return {
            "event_type": "other",
            "companies": companies,
            "date": None,
            "confidence": 0.1,
            "summary": text[:200],
            "error": error,
            **base,
        }
