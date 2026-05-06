# FinStreami - Financial Streaming Platform Setup

## Project Structure

```
finstreami/
├── README.md
├── docker-compose.yml
├── docker-compose.prod.yml
├── .env.example
├── .gitignore
├── Makefile
├── requirements.txt
├── pyproject.toml
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── cd.yml
│       └── security-scan.yml
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   ├── modules/
│   │   │   ├── vpc/
│   │   │   ├── eks/
│   │   │   ├── rds/
│   │   │   ├── kafka/
│   │   │   ├── redis/
│   │   │   └── s3/
│   │   └── environments/
│   │       ├── dev/
│   │       ├── staging/
│   │       └── prod/
│   ├── kubernetes/
│   │   ├── namespace.yaml
│   │   ├── configmaps/
│   │   ├── secrets/
│   │   ├── deployments/
│   │   ├── services/
│   │   ├── ingress/
│   │   └── monitoring/
│   └── helm/
│       └── finstreami/
│           ├── Chart.yaml
│           ├── values.yaml
│           ├── templates/
│           └── charts/
├── data-ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── __init__.py
│   │   ├── config/
│   │   │   ├── __init__.py
│   │   │   ├── kafka_config.py
│   │   │   ├── data_source_config.py
│   │   │   └── schema_registry.py
│   │   ├── producers/
│   │   │   ├── __init__.py
│   │   │   ├── base_producer.py
│   │   │   ├── market_data_producer.py
│   │   │   ├── news_producer.py
│   │   │   ├── social_media_producer.py
│   │   │   ├── sec_filing_producer.py
│   │   │   └── alt_data_producer.py
│   │   ├── schemas/
│   │   │   ├── market_tick.avsc
│   │   │   ├── news_article.avsc
│   │   │   ├── social_post.avsc
│   │   │   └── sec_filing.avsc
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── data_validation.py
│   │       └── monitoring.py
│   └── tests/
├── stream-processing/
│   ├── Dockerfile
│   ├── pom.xml
│   ├── src/main/java/com/finstreami/
│   │   ├── jobs/
│   │   │   ├── DataCleaningJob.java
│   │   │   ├── AggregationJob.java
│   │   │   ├── AnomalyDetectionJob.java
│   │   │   ├── FeatureEngineeringJob.java
│   │   │   ├── RealTimeJoinJob.java
│   │   │   └── SignalGenerationJob.java
│   │   ├── functions/
│   │   │   ├── DataCleaningFunction.java
│   │   │   ├── TechnicalIndicatorFunction.java
│   │   │   └── AnomalyDetectorFunction.java
│   │   ├── sinks/
│   │   │   ├── DeltaLakeSink.java
│   │   │   ├── FeatureStoreSink.java
│   │   │   └── AlertSink.java
│   │   ├── sources/
│   │   │   ├── KafkaSourceFactory.java
│   │   │   └── CustomDeserializers.java
│   │   └── utils/
│   │       ├── ConfigManager.java
│   │       └── MetricsCollector.java
│   └── src/main/resources/
│       ├── application.conf
│       └── log4j2.xml
├── ml-services/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── __init__.py
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── sentiment_analysis/
│   │   │   │   ├── finbert_model.py
│   │   │   │   ├── inference.py
│   │   │   │   └── training.py
│   │   │   ├── event_extraction/
│   │   │   │   ├── llama_model.py
│   │   │   │   ├── inference.py
│   │   │   │   └── fine_tuning.py
│   │   │   ├── market_prediction/
│   │   │   │   ├── multimodal_transformer.py
│   │   │   │   ├── inference.py
│   │   │   │   └── training.py
│   │   │   └── risk_assessment/
│   │   │       ├── xgboost_model.py
│   │   │       ├── neural_net.py
│   │   │       └── inference.py
│   │   ├── serving/
│   │   │   ├── __init__.py
│   │   │   ├── ray_serve_app.py
│   │   │   ├── triton_models/
│   │   │   └── model_endpoints.py
│   │   ├── vector_db/
│   │   │   ├── __init__.py
│   │   │   ├── weaviate_client.py
│   │   │   ├── embedding_service.py
│   │   │   └── similarity_search.py
│   │   └── feature_store/
│   │       ├── __init__.py
│   │       ├── feast_config.py
│   │       ├── feature_definitions.py
│   │       └── online_serving.py
│   └── tests/
├── api-services/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── api/
│   │   │   ├── __init__.py
│   │   │   ├── v1/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── query.py
│   │   │   │   ├── prediction.py
│   │   │   │   ├── analytics.py
│   │   │   │   ├── alerts.py
│   │   │   │   ├── data.py
│   │   │   │   └── users.py
│   │   │   └── middleware/
│   │   │       ├── __init__.py
│   │   │       ├── auth.py
│   │   │       ├── rate_limit.py
│   │   │       └── logging.py
│   │   ├── core/
│   │   │   ├── __init__.py
│   │   │   ├── config.py
│   │   │   ├── database.py
│   │   │   ├── security.py
│   │   │   └── dependencies.py
│   │   ├── models/
│   │   │   ├── __init__.py
│   │   │   ├── user.py
│   │   │   ├── query.py
│   │   │   ├── prediction.py
│   │   │   └── analytics.py
│   │   ├── services/
│   │   │   ├── __init__.py
│   │   │   ├── query_service.py
│   │   │   ├── prediction_service.py
│   │   │   ├── analytics_service.py
│   │   │   └── alert_service.py
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── nlp_processor.py
│   │       └── cache.py
│   └── tests/
├── frontend/
│   ├── Dockerfile
│   ├── package.json
│   ├── tsconfig.json
│   ├── vite.config.ts
│   ├── tailwind.config.js
│   ├── src/
│   │   ├── main.tsx
│   │   ├── App.tsx
│   │   ├── components/
│   │   │   ├── common/
│   │   │   │   ├── Header.tsx
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   └── Layout.tsx
│   │   │   ├── dashboard/
│   │   │   │   ├── Dashboard.tsx
│   │   │   │   ├── PriceChart.tsx
│   │   │   │   ├── SentimentTimeline.tsx
│   │   │   │   ├── EventFeed.tsx
│   │   │   │   ├── RiskMetrics.tsx
│   │   │   │   └── TradingSignals.tsx
│   │   │   ├── advanced/
│   │   │   │   ├── QueryInterface.tsx
│   │   │   │   ├── AlertBuilder.tsx
│   │   │   │   ├── PortfolioOptimizer.tsx
│   │   │   │   └── ModelExplainer.tsx
│   │   │   └── auth/
│   │   │       ├── Login.tsx
│   │   │       └── Register.tsx
│   │   ├── hooks/
│   │   │   ├── useWebSocket.ts
│   │   │   ├── useAuth.ts
│   │   │   └── useRealTimeData.ts
│   │   ├── store/
│   │   │   ├── index.ts
│   │   │   ├── authSlice.ts
│   │   │   ├── dataSlice.ts
│   │   │   └── uiSlice.ts
│   │   ├── services/
│   │   │   ├── api.ts
│   │   │   ├── websocket.ts
│   │   │   └── auth.ts
│   │   ├── types/
│   │   │   ├── api.ts
│   │   │   ├── auth.ts
│   │   │   └── data.ts
│   │   └── utils/
│   │       ├── formatters.ts
│   │       ├── constants.ts
│   │       └── helpers.ts
│   ├── public/
│   └── tests/
├── monitoring/
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alert_rules.yml
│   ├── grafana/
│   │   ├── dashboards/
│   │   │   ├── system-overview.json
│   │   │   ├── ml-performance.json
│   │   │   └── business-metrics.json
│   │   └── provisioning/
│   ├── jaeger/
│   │   └── jaeger-config.yml
│   └── alertmanager/
│       └── alertmanager.yml
├── scripts/
│   ├── setup.sh
│   ├── deploy.sh
│   ├── backup.sh
│   ├── monitoring.sh
│   └── data-migration/
│       ├── migrate-to-delta.py
│       └── seed-data.py
└── docs/
    ├── architecture.md
    ├── api-reference.md
    ├── deployment.md
    ├── monitoring.md
    └── development.md
```

## Week 1-2 Setup Tasks

### 1. GitHub Repository Setup

```bash
# Initialize repository
git init
git remote add origin https://github.com/your-org/finstreami.git

# Create initial branch structure
git checkout -b main
git checkout -b develop
git checkout -b feature/initial-setup
```

### 2. AWS Infrastructure Setup

#### Prerequisites

- AWS CLI configured
- Terraform installed
- kubectl installed
- Docker installed

#### Core AWS Services Required

```hcl
# infrastructure/terraform/main.tf
provider "aws" {
  region = var.aws_region
}

# VPC and Networking
module "vpc" {
  source = "./modules/vpc"

  cidr_block           = "10.0.0.0/16"
  availability_zones   = ["us-east-1a", "us-east-1b", "us-east-1c"]
  public_subnet_cidrs  = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  private_subnet_cidrs = ["10.0.11.0/24", "10.0.12.0/24", "10.0.13.0/24"]
}

# EKS Cluster
module "eks" {
  source = "./modules/eks"

  cluster_name    = "finstreami-cluster"
  cluster_version = "1.28"
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnet_ids

  node_groups = {
    general = {
      instance_types = ["t3.medium"]
      min_size      = 2
      max_size      = 10
      desired_size  = 3
    }
    ml_workload = {
      instance_types = ["g4dn.xlarge"]
      min_size      = 1
      max_size      = 5
      desired_size  = 2
    }
  }
}

# RDS for PostgreSQL
module "rds" {
  source = "./modules/rds"

  identifier     = "finstreami-postgres"
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = "db.r6g.large"
  storage_size   = 100

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
}

# MSK for Kafka
module "kafka" {
  source = "./modules/kafka"

  cluster_name   = "finstreami-kafka"
  kafka_version  = "3.4.0"
  instance_type  = "kafka.m5.large"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
}

# ElastiCache for Redis
module "redis" {
  source = "./modules/redis"

  cluster_id       = "finstreami-redis"
  node_type        = "cache.r6g.large"
  num_cache_nodes  = 3

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
}

# S3 Data Lake
module "s3" {
  source = "./modules/s3"

  bucket_name = "finstreami-datalake"

  lifecycle_rules = {
    bronze_to_ia = {
      days = 30
      storage_class = "STANDARD_IA"
    }
    silver_to_glacier = {
      days = 90
      storage_class = "GLACIER"
    }
  }
}

# TimescaleDB on RDS
resource "aws_db_instance" "timescaledb" {
  identifier     = "finstreami-timescaledb"
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = "db.r6g.xlarge"

  allocated_storage = 200
  storage_encrypted = true

  db_name  = "timescaledb"
  username = var.db_username
  password = var.db_password

  vpc_security_group_ids = [aws_security_group.timescaledb.id]
  db_subnet_group_name   = aws_db_subnet_group.main.name

  backup_retention_period = 7
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"

  tags = {
    Name = "finstreami-timescaledb"
  }
}
```

### 3. Docker Configuration

#### Main Docker Compose for Development

```yaml
# docker-compose.yml
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
      KAFKA_MESSAGE_MAX_BYTES: 1048576
    volumes:
      - kafka-data:/var/lib/kafka/data

  schema-registry:
    image: confluentinc/cp-schema-registry:7.4.0
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes

  postgres:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: finstreami
      POSTGRES_USER: finstreami
      POSTGRES_PASSWORD: finstreami123
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./scripts/init-db.sql:/docker-entrypoint-initdb.d/init-db.sql

  timescaledb:
    image: timescale/timescaledb:latest-pg15
    ports:
      - "5433:5432"
    environment:
      POSTGRES_DB: timescaledb
      POSTGRES_USER: timescale
      POSTGRES_PASSWORD: timescale123
    volumes:
      - timescaledb-data:/var/lib/postgresql/data
      - ./scripts/init-timescaledb.sql:/docker-entrypoint-initdb.d/init-timescaledb.sql

  neo4j:
    image: neo4j:5.12-community
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      NEO4J_AUTH: neo4j/finstreami123
      NEO4J_PLUGINS: '["apoc"]'
    volumes:
      - neo4j-data:/data

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: finstreami
      MINIO_ROOT_PASSWORD: finstreami123
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: finstreami123
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "14268:14268"
      - "16686:16686"
    environment:
      COLLECTOR_OTLP_ENABLED: true

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:
  redis-data:
  postgres-data:
  timescaledb-data:
  neo4j-data:
  minio-data:
  prometheus-data:
  grafana-data:
```

### 4. Environment Configuration

#### Environment Variables Template

```bash
# .env.example
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=finstreami
POSTGRES_USER=finstreami
POSTGRES_PASSWORD=finstreami123

TIMESCALEDB_HOST=localhost
TIMESCALEDB_PORT=5433
TIMESCALEDB_DB=timescaledb
TIMESCALEDB_USER=timescale
TIMESCALEDB_PASSWORD=timescale123

REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=finstreami123

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Data Sources API Keys
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
IEX_CLOUD_TOKEN=your_iex_cloud_token
POLYGON_API_KEY=your_polygon_key
NEWS_API_KEY=your_news_api_key
TWITTER_BEARER_TOKEN=your_twitter_bearer_token

# ML Model Configuration
HUGGING_FACE_TOKEN=your_hf_token
OPENAI_API_KEY=your_openai_key

# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
S3_BUCKET=finstreami-datalake

# Security
JWT_SECRET_KEY=your_super_secret_jwt_key_here
ENCRYPTION_KEY=your_encryption_key_here

# Monitoring
PROMETHEUS_URL=http://localhost:9090
GRAFANA_URL=http://localhost:3000
JAEGER_ENDPOINT=http://localhost:14268/api/traces

# Application Settings
API_V1_PREFIX=/api/v1
DEBUG=true
LOG_LEVEL=INFO
CORS_ORIGINS=["http://localhost:3000", "http://localhost:5173"]
```

### 5. Package Requirements

#### Python Requirements

```txt
# requirements.txt
# FastAPI and API dependencies
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.0
pydantic-settings==2.1.0

# Database drivers
psycopg2-binary==2.9.7
sqlalchemy==2.0.23
alembic==1.12.1
redis==5.0.1
neo4j==5.14.1

# Kafka and streaming
confluent-kafka==2.3.0
kafka-python==2.0.2
avro-python3==1.11.3

# ML and AI libraries
torch==2.1.1
transformers==4.36.0
sentence-transformers==2.2.2
scikit-learn==1.3.2
xgboost==2.0.2
numpy==1.24.3
pandas==2.1.3

# Data processing
delta-spark==3.0.0
pyspark==3.5.0
apache-flink==1.18.0

# Vector databases
weaviate-client==3.25.3
faiss-cpu==1.7.4

# Feature store
feast==0.34.1

# Monitoring and observability
prometheus-client==0.19.0
opentelemetry-api==1.21.0
opentelemetry-sdk==1.21.0
opentelemetry-instrumentation-fastapi==0.42b0
opentelemetry-exporter-jaeger==1.21.0

# Authentication and security
python-jose[cryptography]==3.3.0
passlib[bcrypt]==1.7.4
python-multipart==0.0.6

# HTTP clients and utilities
httpx==0.25.2
requests==2.31.0
aiofiles==23.2.1
python-dotenv==1.0.0

# Testing
pytest==7.4.3
pytest-asyncio==0.21.1
httpx==0.25.2

# Utilities
loguru==0.7.2
typer==0.9.0
rich==13.7.0
```

#### Frontend Package Configuration

```json
{
  "name": "finstreami-frontend",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview",
    "test": "vitest",
    "lint": "eslint . --ext ts,tsx --report-unused-disable-directives --max-warnings 0",
    "type-check": "tsc --noEmit"
  },
  "dependencies": {
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "@reduxjs/toolkit": "^1.9.7",
    "react-redux": "^8.1.3",
    "@tanstack/react-query": "^4.36.1",
    "react-router-dom": "^6.18.0",
    "socket.io-client": "^4.7.4",
    "recharts": "^2.8.0",
    "lightweight-charts": "^4.1.3",
    "@headlessui/react": "^1.7.17",
    "@heroicons/react": "^2.0.18",
    "clsx": "^2.0.0",
    "date-fns": "^2.30.0",
    "zod": "^3.22.4",
    "react-hook-form": "^7.47.0",
    "@hookform/resolvers": "^3.3.2"
  },
  "devDependencies": {
    "@types/react": "^18.2.37",
    "@types/react-dom": "^18.2.15",
    "@typescript-eslint/eslint-plugin": "^6.10.0",
    "@typescript-eslint/parser": "^6.10.0",
    "@vitejs/plugin-react": "^4.1.0",
    "eslint": "^8.53.0",
    "eslint-plugin-react-hooks": "^4.6.0",
    "eslint-plugin-react-refresh": "^0.4.4",
    "typescript": "^5.2.2",
    "vite": "^4.5.0",
    "vitest": "^0.34.6",
    "tailwindcss": "^3.3.5",
    "autoprefixer": "^10.4.16",
    "postcss": "^8.4.31"
  }
}
```

### 6. Initial Setup Scripts

#### Setup Script

```bash
#!/bin/bash
# scripts/setup.sh

set -e

echo "🚀 Setting up FinStreami development environment..."

# Check prerequisites
echo "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "❌ Docker is required"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "❌ Docker Compose is required"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "❌ Python 3 is required"; exit 1; }
command -v node >/dev/null 2>&1 || { echo "❌ Node.js is required"; exit 1; }

echo "✅ Prerequisites check passed"

# Create environment file
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please update .env file with your API keys and configuration"
fi

# Set up Python virtual environment
echo "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Set up pre-commit hooks
echo "Setting up pre-commit hooks..."
pip install pre-commit
pre-commit install

# Install frontend dependencies
echo "Installing frontend dependencies..."
cd frontend
npm install
cd ..

# Start infrastructure services
echo "Starting infrastructure services..."
docker-compose up -d zookeeper kafka schema-registry redis postgres timescaledb neo4j minio

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 30

# Create Kafka topics
echo "Creating Kafka topics..."
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic market.ticks.raw --partitions 50 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic market.ticks.clean --partitions 50 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic market.bars.1min --partitions 20 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic news.articles.raw --partitions 10 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic news.articles.scored --partitions 10 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic social.posts.raw --partitions 15 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic social.sentiment --partitions 15 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic events.extracted --partitions 5 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic alerts.anomalies --partitions 5 --replication-factor 1
docker-compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic predictions.signals --partitions 10 --replication-factor 1

# Initialize databases
echo "Initializing databases..."
python scripts/data-migration/seed-data.py

# Start monitoring services
echo "Starting monitoring services..."
docker-compose up -d prometheus grafana jaeger

echo "✅ Setup completed successfully!"
echo ""
echo "🎯 Next steps:"
echo "1. Update .env file with your API keys"
echo "2. Start development services: make dev"
echo "3. Access services:"
echo "   - API: http://localhost:8000"
echo "   - Frontend: http://localhost:3000"
echo "   - Grafana: http://localhost:3000 (admin/finstreami123)"
echo "   - Prometheus: http://localhost:9090"
echo "   - Jaeger: http://localhost:16686"
echo "   - Kafka UI: docker-compose up kafka-ui (port 8080)"
```

#### Makefile for Easy Commands

```makefile
# Makefile
.PHONY: help setup dev test build deploy clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $1, $2}' $(MAKEFILE_LIST)

setup: ## Set up the development environment
	@echo "Setting up development environment..."
	./scripts/setup.sh

dev: ## Start development environment
	@echo "Starting development environment..."
	docker-compose up -d
	@echo "Starting API services..."
	cd api-services && python -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000 &
	@echo "Starting frontend..."
	cd frontend && npm run dev &
	@echo "Development environment started!"

test: ## Run all tests
	@echo "Running tests..."
	cd api-services && python -m pytest tests/ -v
	cd frontend && npm test
	cd ml-services && python -m pytest tests/ -v

build: ## Build all services
	@echo "Building services..."
	docker-compose -f docker-compose.prod.yml build

deploy-dev: ## Deploy to development environment
	@echo "Deploying to development..."
	cd infrastructure/terraform/environments/dev && terraform apply

deploy-prod: ## Deploy to production environment
	@echo "Deploying to production..."
	cd infrastructure/terraform/environments/prod && terraform apply

clean: ## Clean up development environment
	@echo "Cleaning up..."
	docker-compose down -v
	docker system prune -f

logs: ## Show logs from all services
	docker-compose logs -f

kafka-ui: ## Start Kafka UI
	docker run -d --name kafka-ui -p 8080:8080 \
		-e KAFKA_CLUSTERS_0_NAME=local \
		-e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=host.docker.internal:9092 \
		-e KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://host.docker.internal:8081 \
		provectuslabs/kafka-ui:latest

monitor: ## Open monitoring dashboards
	@echo "Opening monitoring dashboards..."
	open http://localhost:3000  # Grafana
	open http://localhost:9090  # Prometheus
	open http://localhost:16686 # Jaeger
	open http://localhost:8080  # Kafka UI

backup: ## Backup databases
	./scripts/backup.sh

restore: ## Restore from backup
	./scripts/restore.sh $(BACKUP_FILE)

init-aws: ## Initialize AWS infrastructure
	cd infrastructure/terraform && terraform init
	cd infrastructure/terraform/environments/dev && terraform init

plan-aws: ## Plan AWS infrastructure changes
	cd infrastructure/terraform/environments/dev && terraform plan

apply-aws: ## Apply AWS infrastructure changes
	cd infrastructure/terraform/environments/dev && terraform apply

destroy-aws: ## Destroy AWS infrastructure (be careful!)
	cd infrastructure/terraform/environments/dev && terraform destroy
```

### 7. Database Initialization Scripts

#### PostgreSQL Initialization

```sql
-- scripts/init-db.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

-- Users and Authentication
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    is_superuser BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- User Sessions
CREATE TABLE user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    token_hash VARCHAR(255) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Model Metadata
CREATE TABLE ml_models (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    version VARCHAR(50) NOT NULL,
    model_type VARCHAR(100) NOT NULL,
    config JSONB,
    performance_metrics JSONB,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(name, version)
);

-- Data Lineage
CREATE TABLE data_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_dataset VARCHAR(255) NOT NULL,
    target_dataset VARCHAR(255) NOT NULL,
    transformation_logic TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Job Scheduling
CREATE TABLE scheduled_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name VARCHAR(255) NOT NULL,
    job_type VARCHAR(100) NOT NULL,
    schedule_expression VARCHAR(255), -- Cron expression
    config JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Audit Logs
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    action VARCHAR(255) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    resource_id VARCHAR(255),
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_expires_at ON user_sessions(expires_at);
CREATE INDEX idx_ml_models_name_version ON ml_models(name, version);
CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);
```

#### TimescaleDB Initialization

```sql
-- scripts/init-timescaledb.sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Market Data Tables
CREATE TABLE market_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(15,6) NOT NULL,
    volume INTEGER NOT NULL,
    bid_price DECIMAL(15,6),
    ask_price DECIMAL(15,6),
    bid_size INTEGER,
    ask_size INTEGER,
    exchange VARCHAR(10)
);

CREATE TABLE market_bars (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(10) NOT NULL, -- 1min, 5min, 1hour, etc.
    open_price DECIMAL(15,6) NOT NULL,
    high_price DECIMAL(15,6) NOT NULL,
    low_price DECIMAL(15,6) NOT NULL,
    close_price DECIMAL(15,6) NOT NULL,
    volume BIGINT NOT NULL,
    vwap DECIMAL(15,6),
    trade_count INTEGER
);

-- Technical Indicators
CREATE TABLE technical_indicators (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    indicator_name VARCHAR(50) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    value DECIMAL(15,6) NOT NULL,
    metadata JSONB
);

-- Sentiment Scores
CREATE TABLE sentiment_scores (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    source VARCHAR(50) NOT NULL,
    sentiment_score DECIMAL(5,4) NOT NULL, -- -1 to 1
    confidence_score DECIMAL(5,4),
    article_count INTEGER DEFAULT 1
);

-- Trading Signals
CREATE TABLE trading_signals (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    signal_type VARCHAR(50) NOT NULL,
    signal_strength DECIMAL(5,4) NOT NULL, -- 0 to 1
    direction VARCHAR(10) NOT NULL, -- buy/sell/hold
    confidence DECIMAL(5,4),
    metadata JSONB
);

-- Risk Metrics
CREATE TABLE risk_metrics (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    var_1d DECIMAL(15,6),
    var_5d DECIMAL(15,6),
    expected_shortfall DECIMAL(15,6),
    beta DECIMAL(10,6),
    sharpe_ratio DECIMAL(10,6),
    max_drawdown DECIMAL(10,6)
);

-- System Metrics
CREATE TABLE system_metrics (
    time TIMESTAMPTZ NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,6) NOT NULL,
    tags JSONB
);

-- Create hypertables
SELECT create_hypertable('market_ticks', 'time');
SELECT create_hypertable('market_bars', 'time');
SELECT create_hypertable('technical_indicators', 'time');
SELECT create_hypertable('sentiment_scores', 'time');
SELECT create_hypertable('trading_signals', 'time');
SELECT create_hypertable('risk_metrics', 'time');
SELECT create_hypertable('system_metrics', 'time');

-- Create indexes
CREATE INDEX idx_market_ticks_symbol_time ON market_ticks (symbol, time DESC);
CREATE INDEX idx_market_bars_symbol_timeframe_time ON market_bars (symbol, timeframe, time DESC);
CREATE INDEX idx_technical_indicators_symbol_indicator ON technical_indicators (symbol, indicator_name, time DESC);
CREATE INDEX idx_sentiment_scores_symbol_source ON sentiment_scores (symbol, source, time DESC);
CREATE INDEX idx_trading_signals_symbol_type ON trading_signals (symbol, signal_type, time DESC);
CREATE INDEX idx_risk_metrics_symbol_time ON risk_metrics (symbol, time DESC);
CREATE INDEX idx_system_metrics_service_metric ON system_metrics (service_name, metric_name, time DESC);

-- Create continuous aggregates for common queries
CREATE MATERIALIZED VIEW market_bars_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    symbol,
    first(open_price, time) AS open_price,
    max(high_price) AS high_price,
    min(low_price) AS low_price,
    last(close_price, time) AS close_price,
    sum(volume) AS volume,
    avg(vwap) AS vwap,
    sum(trade_count) AS trade_count
FROM market_bars
WHERE timeframe = '1min'
GROUP BY hour, symbol;

CREATE MATERIALIZED VIEW sentiment_daily_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    symbol,
    source,
    avg(sentiment_score) AS avg_sentiment,
    avg(confidence_score) AS avg_confidence,
    sum(article_count) AS total_articles
FROM sentiment_scores
GROUP BY day, symbol, source;

-- Retention policies
SELECT add_retention_policy('market_ticks', INTERVAL '7 days');
SELECT add_retention_policy('system_metrics', INTERVAL '30 days');
```

### 8. CI/CD Pipeline Configuration

#### GitHub Actions Workflow

```yaml
# .github/workflows/ci.yml
name: CI Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  test-python:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.9, 3.10, 3.11]

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install -r api-services/requirements.txt
          pip install -r ml-services/requirements.txt

      - name: Run tests
        run: |
          cd api-services && python -m pytest tests/ --cov=src --cov-report=xml
          cd ../ml-services && python -m pytest tests/ --cov=src --cov-report=xml

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3

  test-frontend:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: "18"
          cache: "npm"
          cache-dependency-path: frontend/package-lock.json

      - name: Install dependencies
        run: |
          cd frontend
          npm ci

      - name: Run tests
        run: |
          cd frontend
          npm run test
          npm run build

  security-scan:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Run Trivy vulnerability scanner
        uses: aquasecurity/trivy-action@master
        with:
          scan-type: "fs"
          scan-ref: "."
          format: "sarif"
          output: "trivy-results.sarif"

      - name: Upload Trivy scan results to GitHub Security tab
        uses: github/codeql-action/upload-sarif@v2
        with:
          sarif_file: "trivy-results.sarif"

  build-and-push:
    needs: [test-python, test-frontend]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'

    strategy:
      matrix:
        service:
          [
            api-services,
            ml-services,
            frontend,
            data-ingestion,
            stream-processing,
          ]

    steps:
      - uses: actions/checkout@v4

      - name: Log in to Container Registry
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}/${{ matrix.service }}
          tags: |
            type=ref,event=branch
            type=ref,event=pr
            type=sha,prefix={{branch}}-
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push Docker image
        uses: docker/build-push-action@v5
        with:
          context: ./${{ matrix.service }}
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

### 9. Monitoring Configuration

#### Prometheus Configuration

```yaml
# monitoring/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "alert_rules.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - alertmanager:9093

scrape_configs:
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "api-services"
    static_configs:
      - targets: ["api-services:8000"]
    metrics_path: "/metrics"
    scrape_interval: 5s

  - job_name: "ml-services"
    static_configs:
      - targets: ["ml-services:8001"]
    metrics_path: "/metrics"
    scrape_interval: 10s

  - job_name: "kafka"
    static_configs:
      - targets: ["kafka:9092"]
    metrics_path: "/metrics"

  - job_name: "redis"
    static_configs:
      - targets: ["redis:6379"]

  - job_name: "postgres"
    static_configs:
      - targets: ["postgres:5432"]

  - job_name: "node-exporter"
    static_configs:
      - targets: ["node-exporter:9100"]

  - job_name: "cadvisor"
    static_configs:
      - targets: ["cadvisor:8080"]
```

### 10. Development Workflow

#### Git Hooks Configuration

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-merge-conflict

  - repo: https://github.com/psf/black
    rev: 23.11.0
    hooks:
      - id: black
        language_version: python3
        files: ^(api-services|ml-services|data-ingestion)/

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        files: ^(api-services|ml-services|data-ingestion)/

  - repo: https://github.com/pycqa/flake8
    rev: 6.1.0
    hooks:
      - id: flake8
        files: ^(api-services|ml-services|data-ingestion)/

  - repo: https://github.com/pre-commit/mirrors-eslint
    rev: v8.54.0
    hooks:
      - id: eslint
        files: ^frontend/
        additional_dependencies:
          - "@typescript-eslint/parser"
          - "@typescript-eslint/eslint-plugin"
```

### 11. API Service Foundation

#### FastAPI Main Application

```python
# api-services/src/main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from contextlib import asynccontextmanager
import time

from .core.config import get_settings
from .core.database import init_db
from .api.v1 import query, prediction, analytics, alerts, data, users
from .api.middleware.logging import LoggingMiddleware
from .api.middleware.auth import AuthMiddleware
from .api.middleware.rate_limit import RateLimitMiddleware

settings = get_settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    yield
    # Shutdown
    pass

app = FastAPI(
    title="FinStreami API",
    description="Real-time Financial Data Streaming and Analytics Platform",
    version="1.0.0",
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    lifespan=lifespan
)

# Security middleware
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.ALLOWED_HOSTS)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom middleware
app.add_middleware(LoggingMiddleware)
app.add_middleware(AuthMiddleware)
app.add_middleware(RateLimitMiddleware)

# Add request timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Include routers
app.include_router(
    query.router,
    prefix=f"{settings.API_V1_PREFIX}/query",
    tags=["query"]
)
app.include_router(
    prediction.router,
    prefix=f"{settings.API_V1_PREFIX}/predict",
    tags=["prediction"]
)
app.include_router(
    analytics.router,
    prefix=f"{settings.API_V1_PREFIX}/analytics",
    tags=["analytics"]
)
app.include_router(
    alerts.router,
    prefix=f"{settings.API_V1_PREFIX}/alerts",
    tags=["alerts"]
)
app.include_router(
    data.router,
    prefix=f"{settings.API_V1_PREFIX}/data",
    tags=["data"]
)
app.include_router(
    users.router,
    prefix=f"{settings.API_V1_PREFIX}/users",
    tags=["users"]
)

@app.get("/")
async def root():
    return {
        "message": "FinStreami API",
        "version": "1.0.0",
        "status": "healthy"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": time.time()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
```

## Next Steps for Week 1-2

1. **Repository Setup**: Create GitHub repository and push initial structure
2. **Environment Setup**: Run setup script and configure development environment
3. **AWS Infrastructure**: Initialize Terraform and create basic AWS resources
4. **Local Development**: Start with docker-compose for local development
5. **Basic Services**: Implement basic API endpoints and data ingestion
6. **Testing**: Set up testing framework and basic tests
7. **Monitoring**: Configure basic monitoring and logging
8. **Documentation**: Create comprehensive documentation for the setup

## Commands to Get Started

```bash
# Clone and setup
git clone <repository-url>
cd finstreami
chmod +x scripts/setup.sh
make setup

# Start development environment
make dev

# Access services
# API: http://localhost:8000
# Frontend: http://localhost:3000
# Grafana: http://localhost:3000
# Kafka UI: make kafka-ui (http://localhost:8080)
```

This setup provides a solid foundation for your financial streaming platform with proper separation of concerns, scalability considerations, and production-ready configurations.
