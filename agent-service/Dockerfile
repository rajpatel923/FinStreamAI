FROM python:3.11-slim

WORKDIR /app

# Copy shared config
COPY finstream_config/ ./finstream_config/

# Copy service files
COPY agent-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent-service/src/ ./src/

EXPOSE 8006

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8006"]
