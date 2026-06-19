FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY data-ingestion/requirements.txt .
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

COPY finstream_config/ ./finstream_config/
COPY data-ingestion/src/ ./src/

CMD ["python", "-m", "src.main"]
