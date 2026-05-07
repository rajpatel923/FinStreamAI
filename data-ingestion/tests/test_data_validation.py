"""Tests for DataValidator — valid/invalid records and boundary cases."""
import math
import time

import pytest

from src.utils.data_validation import DataValidator

validator = DataValidator()


class TestValidateMarketTick:
    def test_valid_tick(self, sample_tick):
        result = validator.validate_market_tick(sample_tick)
        assert result.valid
        assert result.errors == []

    def test_price_zero_is_invalid(self, sample_tick):
        sample_tick["price"] = 0
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid
        assert any("price" in e for e in result.errors)

    def test_price_negative_is_invalid(self, sample_tick):
        sample_tick["price"] = -5.0
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_price_nan_is_invalid(self, sample_tick):
        sample_tick["price"] = math.nan
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_price_inf_is_invalid(self, sample_tick):
        sample_tick["price"] = math.inf
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_volume_negative_is_invalid(self, sample_tick):
        sample_tick["volume"] = -1
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_volume_zero_is_valid(self, sample_tick):
        sample_tick["volume"] = 0
        result = validator.validate_market_tick(sample_tick)
        assert result.valid

    def test_empty_symbol_is_invalid(self, sample_tick):
        sample_tick["symbol"] = ""
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_future_timestamp_beyond_tolerance_is_invalid(self, sample_tick):
        sample_tick["timestamp_ms"] = int(time.time() * 1000) + 10_000  # 10s in future
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid

    def test_timestamp_within_tolerance_is_valid(self, sample_tick):
        sample_tick["timestamp_ms"] = int(time.time() * 1000) + 3_000  # 3s in future
        result = validator.validate_market_tick(sample_tick)
        assert result.valid

    def test_multiple_errors_reported(self, sample_tick):
        sample_tick["price"] = -1
        sample_tick["symbol"] = ""
        result = validator.validate_market_tick(sample_tick)
        assert not result.valid
        assert len(result.errors) >= 2


class TestValidateNewsArticle:
    def test_valid_article(self, sample_article):
        result = validator.validate_news_article(sample_article)
        assert result.valid

    def test_empty_headline_invalid(self, sample_article):
        sample_article["headline"] = ""
        result = validator.validate_news_article(sample_article)
        assert not result.valid

    def test_published_ms_zero_invalid(self, sample_article):
        sample_article["published_ms"] = 0
        result = validator.validate_news_article(sample_article)
        assert not result.valid

    def test_published_ms_negative_invalid(self, sample_article):
        sample_article["published_ms"] = -1
        result = validator.validate_news_article(sample_article)
        assert not result.valid

    def test_empty_url_invalid(self, sample_article):
        sample_article["url"] = ""
        result = validator.validate_news_article(sample_article)
        assert not result.valid


class TestValidateSocialPost:
    def test_valid_post(self, sample_post):
        result = validator.validate_social_post(sample_post)
        assert result.valid

    def test_empty_post_id_invalid(self, sample_post):
        sample_post["post_id"] = ""
        result = validator.validate_social_post(sample_post)
        assert not result.valid

    def test_empty_title_invalid(self, sample_post):
        sample_post["title"] = ""
        result = validator.validate_social_post(sample_post)
        assert not result.valid

    def test_created_ms_zero_invalid(self, sample_post):
        sample_post["created_ms"] = 0
        result = validator.validate_social_post(sample_post)
        assert not result.valid


class TestValidateSECFiling:
    def test_valid_filing(self, sample_filing):
        result = validator.validate_sec_filing(sample_filing)
        assert result.valid

    def test_empty_cik_invalid(self, sample_filing):
        sample_filing["cik"] = ""
        result = validator.validate_sec_filing(sample_filing)
        assert not result.valid

    def test_unknown_form_type_invalid(self, sample_filing):
        sample_filing["form_type"] = "UNKNOWN"
        result = validator.validate_sec_filing(sample_filing)
        assert not result.valid

    def test_allowed_form_types(self, sample_filing):
        for form_type in ("8-K", "10-K", "10-Q", "S-1", "4"):
            sample_filing["form_type"] = form_type
            result = validator.validate_sec_filing(sample_filing)
            assert result.valid, f"Expected {form_type} to be valid"

    def test_filed_ms_zero_invalid(self, sample_filing):
        sample_filing["filed_ms"] = 0
        result = validator.validate_sec_filing(sample_filing)
        assert not result.valid
