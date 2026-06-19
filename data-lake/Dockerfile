FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY data-lake/requirements.txt .
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

# Copy shared config (build context = repo root)
COPY finstream_config/ finstream_config/

COPY data-lake/src/ src/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8004

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8004"]
