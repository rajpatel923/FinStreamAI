# Makefile

.PHONY: help setup dev test build deploy clean

help: ## Show this help message
@echo 'Usage: make [target]'
@echo ''
@echo 'Targets:'
@awk 'BEGIN {FS = ":._?## "} /^[a-zA-Z_-]+:._?## / {printf " %-15s %s\n", $1, $2}' $(MAKEFILE_LIST)

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
open http://localhost:3000 # Grafana
open http://localhost:9090 # Prometheus
open http://localhost:16686 # Jaeger
open http://localhost:8080 # Kafka UI

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
