from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Message counters
messages_produced_total = Counter(
    "finstreami_messages_produced_total",
    "Total number of messages successfully produced to Kafka",
    ["producer", "topic"],
)

messages_failed_total = Counter(
    "finstreami_messages_failed_total",
    "Total number of messages that failed to produce",
    ["producer", "topic", "error_type"],
)

bytes_produced_total = Counter(
    "finstreami_bytes_produced_total",
    "Total bytes produced to Kafka",
    ["producer", "topic"],
)

# Latency
fetch_duration_seconds = Histogram(
    "finstreami_fetch_duration_seconds",
    "Time spent fetching data from external APIs",
    ["producer", "source"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

produce_duration_seconds = Histogram(
    "finstreami_produce_duration_seconds",
    "Time spent producing messages to Kafka",
    ["producer", "topic"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

# Circuit breaker
circuit_breaker_state = Gauge(
    "finstreami_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["producer"],
)

# API quota
api_requests_total = Counter(
    "finstreami_api_requests_total",
    "Total API requests to external data sources",
    ["producer", "source", "status"],
)


# Data quality
validation_failures_total = Counter(
    "finstreami_validation_failures_total",
    "Total number of records that failed validation",
    ["producer", "field"],
)

mock_messages_total = Counter(
    "finstreami_mock_messages_total",
    "Total number of mock messages produced",
    ["producer"],
)


def start_metrics_server(port: int = 8001) -> None:
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
