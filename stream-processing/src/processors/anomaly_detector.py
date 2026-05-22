import uuid

from src.config import settings
from src.state.rolling_state import RollingState


class AnomalyDetector:
    """Detects price Z-score anomalies, volume spikes, and price momentum."""

    def __init__(self, rolling_state: RollingState) -> None:
        self._state = rolling_state

    def detect(self, tick: dict) -> list[dict]:
        """Return list of anomaly_alert records (may be empty)."""
        alerts = []
        symbol = tick["symbol"]
        price = tick["price"]
        volume = tick["volume"]
        ts_ms = tick["timestamp_ms"]

        # 1. Price Z-score
        z = self._state.get_zscore(symbol, price)
        if z is not None:
            abs_z = abs(z)
            if abs_z >= settings.ANOMALY_ZSCORE_HIGH:
                state = self._state.get_or_create(symbol)
                prices = list(state.prices)
                window = 30
                recent = prices[-window:] if len(prices) >= window else prices
                mean = sum(recent) / len(recent) if recent else 0.0
                std = (sum((p - mean) ** 2 for p in recent) / len(recent)) ** 0.5 if recent else 0.0
                alerts.append(
                    _make_alert(
                        symbol=symbol,
                        ts_ms=ts_ms,
                        alert_type="PRICE_ZSCORE",
                        severity="HIGH",
                        value=price,
                        threshold=settings.ANOMALY_ZSCORE_HIGH,
                        z_score=z,
                        rolling_mean=mean,
                        rolling_std=std,
                        message=f"Price Z-score {z:.2f} exceeds HIGH threshold {settings.ANOMALY_ZSCORE_HIGH}",
                    )
                )
            elif abs_z >= settings.ANOMALY_ZSCORE_MEDIUM:
                state = self._state.get_or_create(symbol)
                prices = list(state.prices)
                window = 30
                recent = prices[-window:] if len(prices) >= window else prices
                mean = sum(recent) / len(recent) if recent else 0.0
                std = (sum((p - mean) ** 2 for p in recent) / len(recent)) ** 0.5 if recent else 0.0
                alerts.append(
                    _make_alert(
                        symbol=symbol,
                        ts_ms=ts_ms,
                        alert_type="PRICE_ZSCORE",
                        severity="MEDIUM",
                        value=price,
                        threshold=settings.ANOMALY_ZSCORE_MEDIUM,
                        z_score=z,
                        rolling_mean=mean,
                        rolling_std=std,
                        message=f"Price Z-score {z:.2f} exceeds MEDIUM threshold {settings.ANOMALY_ZSCORE_MEDIUM}",
                    )
                )

        # 2. Volume spike
        vol_ratio = self._state.get_volume_ratio(symbol, volume)
        if vol_ratio is not None:
            if vol_ratio >= settings.VOLUME_SPIKE_HIGH_MULT:
                alerts.append(
                    _make_alert(
                        symbol=symbol,
                        ts_ms=ts_ms,
                        alert_type="VOLUME_SPIKE",
                        severity="HIGH",
                        value=float(volume),
                        threshold=settings.VOLUME_SPIKE_HIGH_MULT,
                        message=f"Volume ratio {vol_ratio:.2f}x exceeds HIGH threshold {settings.VOLUME_SPIKE_HIGH_MULT}x",
                    )
                )
            elif vol_ratio >= settings.VOLUME_SPIKE_MEDIUM_MULT:
                alerts.append(
                    _make_alert(
                        symbol=symbol,
                        ts_ms=ts_ms,
                        alert_type="VOLUME_SPIKE",
                        severity="MEDIUM",
                        value=float(volume),
                        threshold=settings.VOLUME_SPIKE_MEDIUM_MULT,
                        message=f"Volume ratio {vol_ratio:.2f}x exceeds MEDIUM threshold {settings.VOLUME_SPIKE_MEDIUM_MULT}x",
                    )
                )

        # 3. Price momentum
        pct_change = self._state.get_price_change_pct(symbol, price, lookback_ms=60_000)
        if pct_change is not None and pct_change >= settings.MOMENTUM_THRESHOLD_PCT:
            alerts.append(
                _make_alert(
                    symbol=symbol,
                    ts_ms=ts_ms,
                    alert_type="PRICE_MOMENTUM",
                    severity="MEDIUM",
                    value=pct_change,
                    threshold=settings.MOMENTUM_THRESHOLD_PCT,
                    message=f"Price momentum {pct_change:.2f}% exceeds threshold {settings.MOMENTUM_THRESHOLD_PCT}%",
                )
            )

        return alerts


def _make_alert(
    symbol: str,
    ts_ms: int,
    alert_type: str,
    severity: str,
    value: float,
    threshold: float,
    message: str,
    z_score: float | None = None,
    rolling_mean: float | None = None,
    rolling_std: float | None = None,
) -> dict:
    return {
        "alert_id": str(uuid.uuid4()),
        "symbol": symbol,
        "timestamp_ms": ts_ms,
        "alert_type": alert_type,
        "severity": severity,
        "value": value,
        "threshold": threshold,
        "z_score": z_score,
        "rolling_mean": rolling_mean,
        "rolling_std": rolling_std,
        "message": message,
        "source_topic": "market.ticks.clean",
    }
