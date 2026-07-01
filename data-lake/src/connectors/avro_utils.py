"""Confluent wire-format Avro decoding for the Bronze-layer Kafka sink."""
from __future__ import annotations

import io
import json
import struct
from pathlib import Path

import fastavro

_SCHEMA_DIR = Path(__file__).parent.parent / "schemas" / "avro"
_MAGIC_BYTE = 0


class AvroDeserializer:
    """Decodes Confluent wire-format Avro bytes (magic byte + schema id + schemaless payload)."""

    def __init__(self) -> None:
        self._schemas: dict[str, dict] = {}

    def _load_schema(self, schema_name: str) -> dict:
        if schema_name not in self._schemas:
            path = _SCHEMA_DIR / f"{schema_name}.avsc"
            with path.open() as f:
                self._schemas[schema_name] = json.load(f)
        return self._schemas[schema_name]

    def deserialize(self, schema_name: str, data: bytes) -> dict:
        schema = self._load_schema(schema_name)
        parsed = fastavro.parse_schema(schema)
        buf = io.BytesIO(data)
        magic, _schema_id = struct.unpack(">bI", buf.read(5))
        if magic != _MAGIC_BYTE:
            raise ValueError(f"Unexpected magic byte: {magic}")
        return fastavro.schemaless_reader(buf, parsed)


avro_deserializer = AvroDeserializer()
