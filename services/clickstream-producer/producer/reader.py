"""Chunked, constant-memory reader for REES46 monthly CSV(.gz) files.

The files are already ordered by event_time (each month ~5 GB, 40-70 M rows), so we stream
record batches with pyarrow instead of loading/sorting the whole file. Within a batch we
re-sort defensively because a handful of rows are out of order at second granularity.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv

COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
]
COLUMN_TYPES = {
    "event_time": pa.string(),
    "event_type": pa.string(),
    "product_id": pa.int64(),
    "category_id": pa.int64(),
    "category_code": pa.string(),
    "brand": pa.string(),
    "price": pa.float64(),
    "user_id": pa.int64(),
    "user_session": pa.string(),
}
_TS_FORMAT = "%Y-%m-%d %H:%M:%S UTC"


def parse_event_time(raw: str) -> datetime:
    """'2019-10-01 00:00:00 UTC' -> tz-aware datetime."""
    return datetime.strptime(raw, _TS_FORMAT).replace(tzinfo=UTC)


def iter_rows(path: str, block_size_mb: int = 32) -> Iterator[dict[str, Any]]:
    reader = pacsv.open_csv(
        path,  # .gz is decompressed transparently (detected from the extension)
        read_options=pacsv.ReadOptions(block_size=block_size_mb << 20),
        convert_options=pacsv.ConvertOptions(
            column_types=COLUMN_TYPES, include_columns=COLUMNS, strings_can_be_null=True
        ),
    )
    for batch in reader:
        rows = batch.to_pylist()
        rows.sort(key=lambda r: r["event_time"] or "")
        for row in rows:
            if row["event_time"]:
                row["event_time"] = parse_event_time(row["event_time"])
                yield row
