import io
import json
import struct
from pathlib import Path

import fastavro
import httpx
import structlog

from src.config.kafka_config import kafka_config

logger = structlog.get_logger(__name__)

_SCHEMA_DIR = Path(__file__).parent / "avro"
_MAGIC_BYTE = 0


class AvroSerializer:
    """Registers schemas with Confluent Schema Registry and serializes/deserializes with fastavro."""

    def __init__(self, schema_registry_url: str) -> None:
        self._url = schema_registry_url.rstrip("/")
        self._schemas: dict[str, dict] = {}       # schema_name -> parsed schema
        self._subject_ids: dict[str, int] = {}    # subject -> schema_id

    def _load_schema(self, schema_name: str) -> dict:
        if schema_name not in self._schemas:
            path = _SCHEMA_DIR / f"{schema_name}.avsc"
            with path.open() as f:
                self._schemas[schema_name] = json.load(f)
        return self._schemas[schema_name]

    def register(self, subject: str, schema_name: str) -> int:
        """Register schema with the Schema Registry and return schema_id (cached)."""
        if subject in self._subject_ids:
            return self._subject_ids[subject]

        schema = self._load_schema(schema_name)
        payload = {"schema": json.dumps(schema)}
        try:
            resp = httpx.post(
                f"{self._url}/subjects/{subject}/versions",
                json=payload,
                timeout=10.0,
            )
            resp.raise_for_status()
            schema_id: int = resp.json()["id"]
            self._subject_ids[subject] = schema_id
            logger.info("Schema registered", subject=subject, schema_id=schema_id)
            return schema_id
        except Exception as exc:
            logger.warning(
                "Schema Registry unavailable, using fallback id=1",
                subject=subject,
                error=str(exc),
            )
            self._subject_ids[subject] = 1
            return 1

    def serialize(self, schema_name: str, record: dict, subject: str | None = None) -> bytes:
        """Encode record with Confluent wire format: magic(1) + schema_id(4) + avro_payload."""
        schema = self._load_schema(schema_name)
        parsed = fastavro.parse_schema(schema)

        effective_subject = subject or f"{schema_name}-value"
        schema_id = self.register(effective_subject, schema_name)

        buf = io.BytesIO()
        buf.write(struct.pack(">bI", _MAGIC_BYTE, schema_id))
        fastavro.schemaless_writer(buf, parsed, record)
        return buf.getvalue()

    def deserialize(self, schema_name: str, data: bytes) -> dict:
        """Decode Confluent wire-format bytes back to a dict."""
        schema = self._load_schema(schema_name)
        parsed = fastavro.parse_schema(schema)

        buf = io.BytesIO(data)
        magic, _schema_id = struct.unpack(">bI", buf.read(5))
        if magic != _MAGIC_BYTE:
            raise ValueError(f"Unexpected magic byte: {magic}")

        return fastavro.schemaless_reader(buf, parsed)


avro_serializer = AvroSerializer(kafka_config.SCHEMA_REGISTRY_URL)
