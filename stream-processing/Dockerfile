FROM python:3.11-slim-bookworm

WORKDIR /app

# confluent-kafka wheels bundle librdkafka; psycopg2-binary bundles libpq
# No apt packages required for pure-Python or pre-compiled wheel installs
COPY stream-processing/requirements.txt .
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

COPY finstream_config/ ./finstream_config/
COPY stream-processing/src/ ./src/

CMD ["python", "-m", "src.main"]
