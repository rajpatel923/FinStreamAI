#!/bin/bash

set -e

echo ".🚀 Setting up FinStreami development environment..."

echo "checking prerequisites..."

command -v docker >/dev/null 2>&1 || { echo "Docker is required but it's not installed. Aborting."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but it's not installed. Aborting."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python3 is required but it's not installed. Aborting."; exit 1; }
command -v node >/dev/null 2>&1 || { echo "Node.js is required but it's not installed. Aborting."; exit 1; }


echo "✅ Prerequisites check passed"

if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "Please review and update the .env file with your configuration."
else
    echo ".env file already exists. Skipping creation."
fi


echo "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

echo "Setting up pre-commit hooks..."
pip install pre-commit
pre-commit install

echo "Installing frontend dependencies..."
cd frontend
npm install
cd ..

echo "Starting infrastructure services with Docker Compose..."
docker-compose up -d zookeeper kafka postgres redis postgres timescaledb neo4j minio

echo "Waiting for services to be ready..."
sleep 30  # Adjust sleep time as necessary for your services to be ready

echo "Setting up the kafka topics..."
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


echo "Initializing databases..."
python scripts/data-migration/seed-data.py

echo "Starting monitoring services..."
docker-compose up -d prometheus grafana jaeger


echo "✅ FinStreami development environment setup complete!"
echo ""
echo "Next steps:"
echo "1. Review and update the .env file with your configuration."
echo "2. Start development services with: make dev"
echo "3. Access Services:"
echo "   - API: http://localhost:8000"
echo "   - Frontend: http://localhost:3000"
echo "   - Grafana: http://localhost:3000 (admin/finstreami123)"
echo "   - Prometheus: http://localhost:9090"
echo "   - Jaeger: http://localhost:16686"
echo "   - Kafka UI: docker-compose up kafka-ui (port 8080)" d