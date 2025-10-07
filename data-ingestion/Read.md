# FinStreamAI Data Ingestion Layer (Go)

High-performance, distributed data ingestion system built in Go for real-time financial data streaming.

## 🏗️ Architecture

```
External APIs → Go Producers → Kafka Cluster → Stream Processing
```

### Components

1. **Market Data Producer**: WebSocket-based real-time stock tick ingestion
2. **News Producer**: REST API polling for financial news
3. **Social Media Producer**: Twitter/Reddit streaming for sentiment analysis
4. **SEC Filing Producer**: Edgar RSS/API for regulatory filings
5. **Alt Data Producer**: Custom data sources (satellite, web scraping, etc.)

## 🚀 Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Kafka 3.5+
- Access to data provider APIs (Polygon.io, NewsAPI, etc.)

### Installation

```bash
# Clone the repository
git clone <repo-url>
cd finstreami-data-ingestion-go

# Install dependencies
make deps

# Copy environment file
cp .env.example .env
# Edit .env with your API keys

# Build all producers
make build
```

### Running Locally

```bash
# Start infrastructure (Kafka, Schema Registry, etc.)
docker-compose up -d zookeeper kafka-1 schema-registry

# Run market producer
make run-market

# Or run all producers
docker-compose up
```

## 📦 Building

```bash
# Build all producers
make build

# Build specific producer
make market-producer

# Build Docker images
make docker-build

# Run tests
make test

# Run integration tests
make test-integration
```

## 🔧 Configuration

Configuration is managed through `configs/config.yaml` and environment variables.

### Environment Variables

```bash
# Kafka
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# Market Data
MARKET_WS_URL=wss://socket.polygon.io/stocks
MARKET_API_KEY=your_polygon_api_key

# News
NEWS_API_KEY=your_newsapi_key
ALPHAVANTAGE_API_KEY=your_alphavantage_key

# Social Media
TWITTER_API_KEY=your_twitter_key
TWITTER_API_SECRET=your_twitter_secret
TWITTER_BEARER_TOKEN=your_bearer_token
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_secret

# Logging
LOG_LEVEL=info
```

## 📊 Monitoring

### Metrics

All producers expose Prometheus metrics at `:9090/metrics`:

- `producer_messages_sent_total`
- `producer_messages_failed_total`
- `producer_latency_seconds`
- `producer_api_calls_total`
- `producer_circuit_breaker_state`

### Dashboards

Access monitoring tools:
- **Kafka UI**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

### Health Checks

Each producer exposes a health endpoint at `:8081/health`

## 🏛️ Data Schemas

### Market Tick
```json
{
  "symbol": "AAPL",
  "price": 150.25,
  "volume": 1000,
  "timestamp": "2025-01-15T10:30:00Z",
  "bid_price": 150.24,
  "ask_price": 150.26,
  "exchange": "NASDAQ"
}
```

### News Article
```json
{
  "id": "uuid",
  "title": "Apple announces...",
  "content": "Full article text",
  "source": "Reuters",
  "url": "https://...",
  "published_at": "2025-01-15T10:00:00Z",
  "sentiment": 0.85
}
```

## 🔒 Production Deployment

### Kafka Topics

Create topics with appropriate partitions and replication:

```bash
# Market ticks (high volume)
kafka-topics --create --topic market.ticks.raw \
  --partitions 16 --replication-factor 3

# News articles (medium volume)
kafka-topics --create --topic news.articles.raw \
  --partitions 8 --replication-factor 3

# Social posts (high volume)
kafka-topics --create --topic social.posts.raw \
  --partitions 12 --replication-factor 3

# SEC filings (low volume)
kafka-topics --create --topic sec.filings.raw \
  --partitions 4 --replication-factor 3
```

### Kubernetes Deployment

```bash
# Build and push images
make docker-build
make docker-push

# Deploy to Kubernetes
kubectl apply -f infrastructure/kubernetes/
```

## 🧪 Testing

```bash
# Unit tests
make test

# Integration tests (requires running Kafka)
make test-integration

# Benchmarks
make bench

# Coverage report
make test
open coverage.html
```

## 🎯 Performance Tuning

### Kafka Producer Settings

- **Batch Size**: 16KB (balance latency vs throughput)
- **Linger Time**: 10ms (aggregate messages)
- **Compression**: Snappy (good speed/ratio)
- **Acks**: All (durability)
- **Idempotence**: Enabled (exactly-once)

### Go Optimization

- Connection pooling for HTTP clients
- Goroutine pools for parallel processing
- Buffer reuse to reduce GC pressure
- Efficient JSON serialization

## 🐛 Troubleshooting

### Common Issues

**Producer fails to connect to Kafka**
```bash
# Check Kafka is running
docker-compose ps

# Verify network connectivity
docker-compose exec market-producer ping kafka-1
```

**High message lag**
```bash
# Check consumer group lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group stream-processing --describe
```

**Rate limit errors**
- Adjust `rate_limit` in config
- Check API provider quotas
- Implement exponential backoff

## 📚 Additional Resources

- [Kafka Go Client Docs](https://github.com/segmentio/kafka-go)
- [Prometheus Go Client](https://github.com/prometheus/client_golang)
- [Project Architecture](../docs/architecture.md)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

MIT License - See LICENSE file

## 📧 Contact

For questions: dev@finstreami.com