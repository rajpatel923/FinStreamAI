from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Message counters
messages_consumed_total = Counter(
    "finstreami_stream_messages_consumed_total",
    "Total messages consumed from Kafka",
    ["job", "topic"],
)

messages_produced_total = Counter(
    "finstreami_stream_messages_produced_total",
    "Total messages produced to Kafka by stream jobs",
    ["job", "topic"],
)

messages_failed_total = Counter(
    "finstreami_stream_messages_failed_total",
    "Total messages that failed processing",
    ["job", "error_type"],
)

bytes_produced_total = Counter(
    "finstreami_stream_bytes_produced_total",
    "Total bytes produced by stream jobs",
    ["job", "topic"],
)

# Processing latency
processing_duration_seconds = Histogram(
    "finstreami_stream_processing_duration_seconds",
    "Time spent processing each message",
    ["job"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# State metrics
window_buffers_active = Gauge(
    "finstreami_stream_window_buffers_active",
    "Number of active window buffers",
    ["job", "timeframe"],
)

# Anomaly counters
anomalies_detected_total = Counter(
    "finstreami_stream_anomalies_detected_total",
    "Total anomalies detected",
    ["alert_type", "severity"],
)

# Signal counters
signals_generated_total = Counter(
    "finstreami_stream_signals_generated_total",
    "Total trading signals generated",
    ["direction", "signal_type"],
)

# Circuit breaker
circuit_breaker_state = Gauge(
    "finstreami_stream_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["job"],
)

# TimescaleDB writes
timescaledb_writes_total = Counter(
    "finstreami_stream_timescaledb_writes_total",
    "Total rows written to TimescaleDB",
    ["table"],
)

timescaledb_write_duration_seconds = Histogram(
    "finstreami_stream_timescaledb_write_duration_seconds",
    "Time spent writing to TimescaleDB",
    ["table"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)


# Redis writes
redis_writes_total = Counter(
    "finstreami_stream_redis_writes_total",
    "Total writes to Redis feature store",
    ["type"],
)

redis_write_duration_seconds = Histogram(
    "finstreami_stream_redis_write_duration_seconds",
    "Time spent writing to Redis",
    ["type"],
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1],
)

redis_write_errors_total = Counter(
    "finstreami_stream_redis_write_errors_total",
    "Total Redis write errors",
    ["type"],
)

# Join metrics
joins_attempted_total = Counter(
    "finstreami_stream_joins_attempted_total",
    "Total bar-sentiment join attempts",
)

joins_with_sentiment = Counter(
    "finstreami_stream_joins_with_sentiment_total",
    "Joins that included sentiment data",
    ["source"],
)


def start_metrics_server(port: int = 8002) -> None:
    start_http_server(port)
