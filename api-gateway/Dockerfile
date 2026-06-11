FROM python:3.11-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Copy shared config package first (for pip install)
COPY finstream_config/ /app/finstream_config/

# Copy gateway
COPY api-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api-gateway/ .

EXPOSE 8005

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8005"]
