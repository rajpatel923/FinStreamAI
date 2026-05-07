import time
import uuid
from datetime import date
from typing import Any

import httpx
import structlog

from src.config.data_source_config import data_source_config
from src.producers.base_producer import BaseProducer
from src.schemas.registry import avro_serializer
from src.utils.data_validation import validator
from src.utils.mock_data import mock_generator
from src.utils.monitoring import mock_messages_total, validation_failures_total

logger = structlog.get_logger(__name__)

# CIK -> (company_name, ticker) for top S&P 500 companies
_CIK_TICKER_MAP: dict[str, tuple[str, str]] = {
    "0000320193": ("Apple Inc.", "AAPL"),
    "0000789019": ("Microsoft Corp", "MSFT"),
    "0001652044": ("Alphabet Inc.", "GOOGL"),
    "0001018724": ("Amazon.com Inc.", "AMZN"),
    "0001318605": ("Tesla Inc.", "TSLA"),
    "0001326801": ("Meta Platforms Inc.", "META"),
    "0001045810": ("NVIDIA Corp", "NVDA"),
    "0000019617": ("JPMorgan Chase & Co.", "JPM"),
    "0000080661": ("Berkshire Hathaway Inc.", "BRK.B"),
    "0000200406": ("Johnson & Johnson", "JNJ"),
    "0000078003": ("Exxon Mobil Corp", "XOM"),
    "0000040987": ("Visa Inc.", "V"),
    "0000101830": ("UnitedHealth Group Inc.", "UNH"),
    "0000070858": ("Procter & Gamble Co.", "PG"),
    "0000051143": ("3M Company", "MMM"),
}

_EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"


class SECFilingProducer(BaseProducer):
    """Produces SEC EDGAR 8-K filings to events.extracted (no API key required)."""

    def __init__(self) -> None:
        super().__init__("sec_filing_producer", "events.extracted")
        cfg = data_source_config
        self._use_mock = cfg.USE_MOCK_DATA
        self._http = httpx.Client(
            timeout=cfg.HTTP_TIMEOUT_SECONDS,
            headers={"User-Agent": "FinStreamAI research@finstream.ai"},
        )
        self._seen: set[str] = set()
        logger.info("SECFilingProducer initialised", use_mock=self._use_mock)

    def data_source_name(self) -> str:
        return "mock" if self._use_mock else "sec_edgar"

    def poll_interval_seconds(self) -> float:
        return 600.0

    def fetch_data(self) -> list[dict]:
        if self._use_mock:
            return [mock_generator.sec_filing() for _ in range(3)]
        return self._fetch_edgar()

    def _fetch_edgar(self) -> list[dict]:
        filings: list[dict] = []
        today = date.today().isoformat()
        try:
            resp = self._http.get(
                _EDGAR_SEARCH_URL,
                params={
                    "q": '"8-K"',
                    "dateRange": "custom",
                    "startdt": today,
                    "forms": "8-K",
                },
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", {}).get("hits", [])
            for hit in hits:
                src = hit.get("_source", {})
                accession = src.get("file_date", "") + "_" + src.get("entity_id", str(uuid.uuid4()))
                if accession in self._seen:
                    continue
                cik = str(src.get("entity_id", "")).zfill(10)
                company_name = src.get("display_names", ["Unknown"])[0] if src.get("display_names") else "Unknown"
                ticker = _CIK_TICKER_MAP.get(cik, (None, None))[1]
                period = src.get("period_of_report", None)
                filing_url = src.get("file_date", "")
                filings.append({
                    "filing_id": accession,
                    "cik": cik,
                    "company_name": company_name,
                    "ticker": ticker,
                    "form_type": "8-K",
                    "filed_ms": int(time.time() * 1000),
                    "period_of_report": period,
                    "filing_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=8-K",
                    "description": src.get("file_date", ""),
                    "is_mock": False,
                })
        except Exception as exc:
            logger.warning("EDGAR fetch failed", error=str(exc))
        return filings

    def transform(self, raw_data: list[dict]) -> list[tuple[str | None, bytes]]:
        results: list[tuple[str | None, bytes]] = []
        for record in raw_data:
            filing_id = record.get("filing_id", "")
            if filing_id and filing_id in self._seen:
                continue

            vr = validator.validate_sec_filing(record)
            if not vr.valid:
                for err in vr.errors:
                    validation_failures_total.labels(
                        producer=self.producer_name, field=err.split()[0]
                    ).inc()
                logger.warning("Invalid SEC filing", errors=vr.errors)
                continue

            if filing_id:
                self._seen.add(filing_id)

            payload = avro_serializer.serialize("sec_filing", record)
            if record.get("is_mock"):
                mock_messages_total.labels(producer=self.producer_name).inc()
            key = record.get("ticker") or record.get("cik")
            results.append((key, payload))
        return results
