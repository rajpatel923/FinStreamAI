"""Tests for SECFilingProducer — mock mode, CIK->ticker mapping, deduplication."""
import time
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import avro_decode


@pytest.fixture
def sec_producer(mock_kafka_producer):
    with patch.dict("os.environ", {"USE_MOCK_DATA": "true"}, clear=False):
        from src.producers.sec_filing_producer import SECFilingProducer
        p = SECFilingProducer()
        p._use_mock = True
        return p


class TestSECFilingProducerMockMode:
    def test_fetch_data_returns_filings(self, sec_producer):
        filings = sec_producer.fetch_data()
        assert isinstance(filings, list)
        assert len(filings) > 0

    def test_filings_marked_as_mock(self, sec_producer):
        filings = sec_producer.fetch_data()
        assert all(f["is_mock"] for f in filings)

    def test_filings_have_required_fields(self, sec_producer):
        filings = sec_producer.fetch_data()
        required = {"filing_id", "cik", "company_name", "form_type", "filed_ms", "filing_url"}
        for f in filings:
            assert required <= f.keys()

    def test_transform_produces_avro_bytes(self, sec_producer):
        filings = sec_producer.fetch_data()
        results = sec_producer.transform(filings)
        assert len(results) > 0
        for key, value in results:
            assert isinstance(value, bytes)

    def test_transform_avro_round_trip(self, sec_producer):
        filings = sec_producer.fetch_data()
        results = sec_producer.transform(filings)
        for key, value in results:
            decoded = avro_decode("sec_filing", value)
            assert decoded["cik"]
            assert decoded["form_type"]
            assert decoded["filed_ms"] > 0

    def test_poll_interval(self, sec_producer):
        assert sec_producer.poll_interval_seconds() == 600.0


class TestCIKTickerMapping:
    def test_known_cik_maps_to_ticker(self):
        from src.producers.sec_filing_producer import _CIK_TICKER_MAP
        assert _CIK_TICKER_MAP["0000320193"] == ("Apple Inc.", "AAPL")
        assert _CIK_TICKER_MAP["0000789019"] == ("Microsoft Corp", "MSFT")

    def test_map_has_multiple_entries(self):
        from src.producers.sec_filing_producer import _CIK_TICKER_MAP
        assert len(_CIK_TICKER_MAP) >= 10


class TestSECDeduplication:
    def test_same_filing_id_not_produced_twice(self, sec_producer, sample_filing):
        r1 = sec_producer.transform([sample_filing])
        assert len(r1) == 1

        r2 = sec_producer.transform([sample_filing])
        assert len(r2) == 0

    def test_different_filing_ids_both_produced(self, sec_producer):
        import uuid, time

        def make_filing(fid: str) -> dict:
            return {
                "filing_id": fid,
                "cik": "0000320193",
                "company_name": "Apple Inc.",
                "ticker": "AAPL",
                "form_type": "8-K",
                "filed_ms": int(time.time() * 1000),
                "period_of_report": "2024-03-31",
                "filing_url": "https://sec.gov/filing",
                "description": "Test filing",
                "is_mock": True,
            }

        results = sec_producer.transform([make_filing("f-001"), make_filing("f-002")])
        assert len(results) == 2


class TestEDGARResponseParsing:
    def test_fetch_edgar_handles_http_error(self, mock_kafka_producer):
        with patch.dict("os.environ", {"USE_MOCK_DATA": "false"}, clear=False):
            from src.producers.sec_filing_producer import SECFilingProducer
            p = SECFilingProducer()
            p._use_mock = False

        with patch.object(p._http, "get", side_effect=Exception("Connection error")):
            filings = p._fetch_edgar()
            assert filings == []
