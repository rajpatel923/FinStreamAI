"""Async export worker: run query, serialize, upload to MinIO, return pre-signed URL."""
from __future__ import annotations

import io
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import structlog
from sqlalchemy import text

from src.core.config import settings

logger = structlog.get_logger(__name__)

_PRESIGNED_EXPIRES = settings.EXPORT_PRESIGNED_URL_EXPIRES_S


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.MINIO_ENDPOINT,
        aws_access_key_id=settings.MINIO_ROOT_USER,
        aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
        region_name="us-east-1",
    )


async def run_export(
    job_id: uuid.UUID,
    user_id: uuid.UUID,
    query_params: dict[str, Any],
    output_format: str,
    db_session_factory,
) -> dict[str, Any]:
    """Execute export in background. Returns result dict for DB update."""
    try:
        async with db_session_factory() as db:
            table = query_params.get("table", "market_bars_1min")
            symbol = query_params.get("symbol")
            from_ts = query_params.get("from_ts")
            to_ts = query_params.get("to_ts")
            limit = min(int(query_params.get("limit", 100000)), 1_000_000)

            params: dict[str, Any] = {"limit": limit}
            where_clauses = []
            if symbol:
                where_clauses.append("symbol = :symbol")
                params["symbol"] = symbol.upper()
            if from_ts:
                where_clauses.append("timestamp >= :from_ts")
                params["from_ts"] = from_ts
            if to_ts:
                where_clauses.append("timestamp <= :to_ts")
                params["to_ts"] = to_ts

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
            sql = text(f"SELECT * FROM {table} {where_sql} ORDER BY timestamp DESC LIMIT :limit")
            result = await db.execute(sql, params)
            rows = result.mappings().all()

        df = pd.DataFrame([dict(r) for r in rows])
        row_count = len(df)

        buf = io.BytesIO()
        if output_format == "csv":
            buf.write(df.to_csv(index=False).encode())
            content_type = "text/csv"
            ext = "csv"
        elif output_format == "parquet":
            df.to_parquet(buf, index=False)
            content_type = "application/octet-stream"
            ext = "parquet"
        else:
            buf.write(json.dumps([dict(r) for r in rows], default=str).encode())
            content_type = "application/json"
            ext = "json"

        buf.seek(0)
        file_size = buf.getbuffer().nbytes
        object_key = f"exports/{user_id}/{job_id}.{ext}"

        s3 = _get_s3_client()
        s3.upload_fileobj(buf, settings.DATALAKE_BUCKET_NAME, object_key, ExtraArgs={"ContentType": content_type})
        download_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.DATALAKE_BUCKET_NAME, "Key": object_key},
            ExpiresIn=_PRESIGNED_EXPIRES,
        )

        return {
            "status": "done",
            "row_count": row_count,
            "file_size_bytes": file_size,
            "download_url": download_url,
            "completed_at": datetime.now(timezone.utc),
            "error_message": None,
        }

    except Exception as exc:
        logger.error("Export failed", job_id=str(job_id), error=str(exc))
        return {
            "status": "failed",
            "error_message": str(exc),
            "completed_at": datetime.now(timezone.utc),
        }
