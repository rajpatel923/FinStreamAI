FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY ai-services/requirements.txt .

# Install torch separately for a dedicated cached layer (large download with NVIDIA deps)
RUN pip install --no-cache-dir --timeout 600 --retries 10 \
    "torch>=2.2.0,<3.0.0"

# Install remaining requirements (torch already satisfied, spacy model included via URL)
RUN pip install --no-cache-dir --timeout 300 --retries 10 -r requirements.txt

# Copy shared config (build context = repo root)
COPY finstream_config/ finstream_config/

COPY ai-services/src/ src/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8003

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8003"]
