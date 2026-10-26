"""Avro helpers for Spark.

Messages produced with Confluent's AvroSerializer use the *wire format*:
    byte 0      : magic byte (0)
    bytes 1..4  : schema id (big-endian int)
    bytes 5..   : Avro binary payload
Spark's `from_avro` expects only the payload, so we strip the 5-byte header and decode
with the reader schema from `schemas/avro/*.avsc` (same file registered in Schema Registry).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", "/opt/app/schemas/avro"))


def load_avsc(name: str) -> str:
    path = SCHEMA_DIR / f"{name}.avsc"
    return json.dumps(json.loads(path.read_text()))


def decode_confluent_avro(col: str, schema_name: str) -> Column:
    """PERMISSIVE: malformed records become NULL -> routed to the DLQ by the caller."""
    payload = F.expr(f"substring({col}, 6, length({col}) - 5)")
    return from_avro(payload, load_avsc(schema_name), {"mode": "PERMISSIVE"})
