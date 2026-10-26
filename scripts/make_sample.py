"""Build a small, session-complete sample of REES46 for dev/CI.

Sampling by user_session (not by row) keeps funnels and sessions intact, so the
sample is still meaningful for sessionization, cart-abandon and conversion metrics.

    python scripts/make_sample.py --input data/raw/2019-Oct.csv.gz --fraction 0.01 --max-days 3
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv


def keep_session(session: str | None, threshold: int) -> bool:
    """Deterministic hash sampling -> the same sessions are picked on every run."""
    if not session:
        return False
    return int(hashlib.md5(session.encode()).hexdigest()[:8], 16) < threshold


def event_dates(batch: pa.RecordBatch) -> list[dt.date]:
    s = pc.utf8_slice_codeunits(batch.column("event_time").cast(pa.string()), 0, 10)
    return [dt.date.fromisoformat(x) for x in s.to_pylist()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw/2019-Oct.csv.gz")
    ap.add_argument("--output", default="data/sample/rees46_sample.csv.gz")
    ap.add_argument("--fraction", type=float, default=0.01)
    ap.add_argument("--max-days", type=int, default=3, help="keep only the first N days")
    args = ap.parse_args()

    threshold = int(args.fraction * 0xFFFFFFFF)
    reader = pacsv.open_csv(
        args.input,
        read_options=pacsv.ReadOptions(block_size=64 << 20),
        convert_options=pacsv.ConvertOptions(strings_can_be_null=True,
                                             column_types={"event_time": pa.string(),
                                                           "user_session": pa.string()}),
    )
    limit, batches = None, []
    for batch in reader:
        dates = event_dates(batch)
        limit = limit or dates[0] + dt.timedelta(days=args.max_days)
        mask = [keep_session(s, threshold) and d < limit
                for s, d in zip(batch.column("user_session").to_pylist(), dates, strict=True)]
        sub = batch.filter(pa.array(mask))
        if sub.num_rows:
            batches.append(sub)
        if dates[-1] >= limit:   # file is time-ordered
            break

    table = pa.Table.from_batches(batches, schema=reader.schema)
    with pa.CompressedOutputStream(args.output, "gzip") as out:
        pacsv.write_csv(table, out)
    n_sessions = len(pc.unique(table.column("user_session")))
    print(f"wrote {table.num_rows:,} rows / {n_sessions:,} sessions -> {args.output}")


if __name__ == "__main__":
    main()
