FROM python:3.11-slim

WORKDIR /app

# Install system deps required by psycopg, confluent-kafka, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Copy shared config
COPY finstream_config/ ./finstream_config/

# Copy service files
COPY agent-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent-service/src/ ./src/

EXPOSE 8006

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8006"]
