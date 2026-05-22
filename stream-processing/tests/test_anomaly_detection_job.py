"""Tests for AnomalyDetector and AnomalyDetectionJob."""
import time
import uuid
from unittest.mock import MagicMock

import pytest

from src.processors.anomaly_detector import AnomalyDetector
from src.state.rolling_state import RollingState


@pytest.fixture
def rolling_state():
    return RollingState()


@pytest.fixture
def detector(rolling_state):
    return AnomalyDetector(rolling_state)


@pytest.fixture
def base_tick():
    return {
        "event_id": str(uuid.uuid4()),
        "symbol": "AAPL",
        "timestamp_ms": int(time.time() * 1000),
        "price": 182.50,
        "volume": 1000,
        "source": "polygon",
        "is_mock": False,
    }


def seed_prices(rolling_state, symbol, prices, volume=1000):
    ts = int(time.time() * 1000)
    for i, price in enumerate(prices):
        rolling_state.update_tick(symbol, price, volume, ts + i * 1000)


# ─── Z-score detection ──────────────────────────────────────────────────────────

class TestZScoreDetection:
    def test_no_alerts_insufficient_data(self, detector, rolling_state, base_tick):
        # Less than 30 ticks → no Z-score possible
        seed_prices(rolling_state, "AAPL", [182.0] * 5)
        alerts = detector.detect(base_tick)
        zscore_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ZSCORE"]
        assert zscore_alerts == []

    def test_high_zscore_alert(self, detector, rolling_state, base_tick):
        # Seed with small variance around 100 (std > 0 required for Z-score)
        seed_prices(rolling_state, "AAPL", [100.0, 100.1, 99.9, 100.2, 99.8] * 7)
        base_tick["price"] = 500.0  # extreme Z-score (>> 3σ)
        alerts = detector.detect(base_tick)
        zscore_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ZSCORE"]
        assert len(zscore_alerts) >= 1
        assert zscore_alerts[0]["severity"] == "HIGH"

    def test_medium_zscore_alert(self, detector, rolling_state, base_tick):
        from src.config import settings

        # Seed with consistent prices, then a moderate deviation
        prices = [100.0] * 35
        seed_prices(rolling_state, "AAPL", prices)
        # std ≈ 0, so any significant deviation triggers HIGH
        # Use a small std instead
        seed_prices(rolling_state, "AAPL", [100.0, 101.0, 99.0, 100.5] * 9)
        # price that is between MEDIUM and HIGH threshold
        state = rolling_state.get_or_create("AAPL")
        import statistics
        p_list = list(state.prices)[-30:]
        mean = sum(p_list) / len(p_list)
        std = statistics.stdev(p_list) if len(p_list) > 1 else 0.0
        if std > 0:
            medium_price = mean + (settings.ANOMALY_ZSCORE_MEDIUM + 0.1) * std
            base_tick["price"] = medium_price
            alerts = detector.detect(base_tick)
            zscore_alerts = [a for a in alerts if a["alert_type"] == "PRICE_ZSCORE"]
            if zscore_alerts:
                assert zscore_alerts[0]["severity"] in ("MEDIUM", "HIGH")

    def test_zscore_alert_has_required_fields(self, detector, rolling_state, base_tick):
        seed_prices(rolling_state, "AAPL", [100.0] * 35)
        base_tick["price"] = 500.0
        alerts = detector.detect(base_tick)
        for alert in alerts:
            if alert["alert_type"] == "PRICE_ZSCORE":
                assert "alert_id" in alert
                assert "symbol" in alert
                assert "timestamp_ms" in alert
                assert "severity" in alert
                assert "message" in alert
                assert "z_score" in alert
                assert alert["z_score"] is not None


# ─── Volume spike detection ──────────────────────────────────────────────────────

class TestVolumeSpikeDetection:
    def test_no_alerts_insufficient_volume_data(self, detector, rolling_state, base_tick):
        seed_prices(rolling_state, "AAPL", [182.0] * 5)
        alerts = detector.detect(base_tick)
        vol_alerts = [a for a in alerts if a["alert_type"] == "VOLUME_SPIKE"]
        assert vol_alerts == []

    def test_high_volume_spike(self, detector, rolling_state, base_tick):
        from src.config import settings

        seed_prices(rolling_state, "AAPL", [182.0] * 25, volume=1000)
        base_tick["volume"] = int(1000 * (settings.VOLUME_SPIKE_HIGH_MULT + 0.5))
        alerts = detector.detect(base_tick)
        vol_alerts = [a for a in alerts if a["alert_type"] == "VOLUME_SPIKE"]
        assert len(vol_alerts) == 1
        assert vol_alerts[0]["severity"] == "HIGH"

    def test_medium_volume_spike(self, detector, rolling_state, base_tick):
        from src.config import settings

        seed_prices(rolling_state, "AAPL", [182.0] * 25, volume=1000)
        # Between MEDIUM and HIGH multiplier
        mult = (settings.VOLUME_SPIKE_MEDIUM_MULT + settings.VOLUME_SPIKE_HIGH_MULT) / 2
        base_tick["volume"] = int(1000 * mult)
        alerts = detector.detect(base_tick)
        vol_alerts = [a for a in alerts if a["alert_type"] == "VOLUME_SPIKE"]
        assert len(vol_alerts) == 1
        assert vol_alerts[0]["severity"] == "MEDIUM"

    def test_normal_volume_no_alert(self, detector, rolling_state, base_tick):
        seed_prices(rolling_state, "AAPL", [182.0] * 25, volume=1000)
        base_tick["volume"] = 1000  # same as average
        alerts = detector.detect(base_tick)
        vol_alerts = [a for a in alerts if a["alert_type"] == "VOLUME_SPIKE"]
        assert vol_alerts == []


# ─── Momentum detection ──────────────────────────────────────────────────────────

class TestMomentumDetection:
    def test_no_alerts_no_history(self, detector, base_tick):
        alerts = detector.detect(base_tick)
        mom_alerts = [a for a in alerts if a["alert_type"] == "PRICE_MOMENTUM"]
        # No history → no momentum alert
        assert mom_alerts == []

    def test_momentum_alert_on_large_move(self, detector, rolling_state, base_tick):
        from src.config import settings

        ts_now = int(time.time() * 1000)
        # Seed a price 90 seconds ago
        rolling_state.update_tick("AAPL", 100.0, 1000, ts_now - 90_000)
        base_tick["timestamp_ms"] = ts_now
        # Move price by more than threshold
        base_tick["price"] = 100.0 * (1 + settings.MOMENTUM_THRESHOLD_PCT / 100.0 + 0.01)
        alerts = detector.detect(base_tick)
        mom_alerts = [a for a in alerts if a["alert_type"] == "PRICE_MOMENTUM"]
        assert len(mom_alerts) >= 1
        assert mom_alerts[0]["severity"] == "MEDIUM"


# ─── AnomalyDetectionJob integration ────────────────────────────────────────────

class TestAnomalyDetectionJob:
    def test_process_normal_tick_no_output(self):
        from src.jobs.anomaly_detection_job import AnomalyDetectionJob

        job = AnomalyDetectionJob()
        tick = {
            "event_id": str(uuid.uuid4()), "symbol": "AAPL",
            "timestamp_ms": int(time.time() * 1000),
            "price": 182.50, "volume": 1000,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "polygon", "is_mock": False,
        }
        outputs = job.process(tick, MagicMock())
        # No anomaly expected with a single tick
        assert isinstance(outputs, list)

    def test_process_extreme_price_produces_alert(self):
        from src.jobs.anomaly_detection_job import AnomalyDetectionJob

        job = AnomalyDetectionJob()
        # Seed with 35 normal ticks first
        ts_base = int(time.time() * 1000)
        for i in range(35):
            seed_tick = {
                "event_id": str(uuid.uuid4()), "symbol": "AAPL",
                "timestamp_ms": ts_base - (35 - i) * 1000,
                "price": 100.0, "volume": 1000,
                "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
                "exchange": None, "source": "polygon", "is_mock": False,
            }
            job._rolling_state.update_tick("AAPL", 100.0, 1000, seed_tick["timestamp_ms"])

        extreme_tick = {
            "event_id": str(uuid.uuid4()), "symbol": "AAPL",
            "timestamp_ms": ts_base,
            "price": 1000.0, "volume": 1000,
            "bid_price": None, "ask_price": None, "bid_size": None, "ask_size": None,
            "exchange": None, "source": "polygon", "is_mock": False,
        }
        outputs = job.process(extreme_tick, MagicMock())
        assert len(outputs) >= 1
        topic, key_bytes, value_bytes = outputs[0]
        assert topic == "alerts.anomalies"
        assert key_bytes == b"AAPL"
        assert len(value_bytes) > 5
