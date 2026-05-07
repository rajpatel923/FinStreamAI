import threading
import time


class TokenBucket:
    """Thread-safe token bucket for API rate limiting."""

    def __init__(self, rate_per_minute: int) -> None:
        self._rate_per_minute = rate_per_minute
        self._tokens = float(rate_per_minute)
        self._max_tokens = float(rate_per_minute)
        self._refill_rate = rate_per_minute / 60.0  # tokens per second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now

    def try_acquire(self) -> bool:
        """Non-blocking: return True if a token was consumed, False otherwise."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def acquire(self) -> None:
        """Block until a token is available, then consume it."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._refill_rate
            time.sleep(wait)
