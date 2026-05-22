from src.state.window_state import WindowBuffer, WindowState


class TickAggregator:
    """Delegates to WindowState — thin coordinator for OHLCV bar aggregation."""

    def __init__(self, window_state: WindowState) -> None:
        self._window_state = window_state

    def ingest(self, tick: dict) -> None:
        self._window_state.update(tick["symbol"], tick)

    def get_closeable_bars(self, now_ms: int) -> list[dict]:
        """Return bar records for all closeable windows, removing them from state."""
        bars = []
        for buf in self._window_state.get_closeable_windows(now_ms):
            bar = WindowState.build_bar_record(buf)
            bars.append(bar)
            self._window_state.remove_window(buf.symbol, buf.timeframe, buf.window_start_ms)
        return bars

    def active_window_count(self) -> int:
        return self._window_state.active_count()
