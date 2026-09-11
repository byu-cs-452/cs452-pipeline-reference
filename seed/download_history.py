"""One-time historical download of the USGS event catalog.

Run this ONCE. It writes parquet chunks to data/raw/ and never touches the
network again for a range it has already saved. Bulk loading into BigQuery is a
separate step (seed/load_seed.py) that reads only from disk.

Why that split matters: USGS is a free public service. A class re-downloading
decades of history on every iteration is how free services stop being free. It
is also 100x slower than reading the parquet you already have.

Sizing strategy
---------------
FDSN caps a response at 20,000 events, so the catalog has to be cut into chunks
that each stay under the cap. A fixed chunk size does not work -- 1975 has a few
thousand events, 2024 has ~156,000. So we recursively bisect any time range whose
/count exceeds the cap. Sparse decades cost one request; dense months get split
until they fit.

Usage:
    python -m seed.download_history --start-year 1970
    python -m seed.download_history --start-year 1970 --resume
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import usgs  # noqa: E402

log = logging.getLogger("seed")

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
MANIFEST = RAW_DIR / "_manifest.json"

# Be a good neighbor: a small pause between requests.
REQUEST_DELAY_SECONDS = 0.4

SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("updated", pa.timestamp("us", tz="UTC")),
        ("mag", pa.float64()),
        ("mag_type", pa.string()),
        ("place", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("depth_km", pa.float64()),
        ("status", pa.string()),
        ("event_type", pa.string()),
        ("net", pa.string()),
        ("tsunami", pa.bool_()),
        ("sig", pa.int64()),
        ("felt", pa.int64()),
        ("cdi", pa.float64()),
        ("mmi", pa.float64()),
        ("alert", pa.string()),
        ("nst", pa.int64()),
        ("dmin", pa.float64()),
        ("rms", pa.float64()),
        ("gap", pa.float64()),
        ("url", pa.string()),
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


def chunk_name(start, end):
    return f"{start:%Y%m%dT%H%M%S}__{end:%Y%m%dT%H%M%S}.parquet"


def plan_chunks(start, end, depth=0):
    """Yield (start, end, count) ranges that each fit in one FDSN request.

    Recursively bisects on /count. The count call is cheap compared to pulling
    20k events, so paying for it to avoid a truncated chunk is a good trade.
    """
    indent = "  " * depth
    count = usgs.count_events(start, end)
    time.sleep(REQUEST_DELAY_SECONDS)

    if count == 0:
        log.info("%s%s -> %s: empty, skipping", indent, start.date(), end.date())
        return

    if count <= usgs.MAX_EVENTS_PER_REQUEST:
        log.info("%s%s -> %s: %d events (fits)", indent, start.date(), end.date(), count)
        yield (start, end, count)
        return

    # Too big. Split in half and try again on each side.
    midpoint = start + (end - start) / 2
    # Guard against pathological splitting on a range shorter than a second.
    if midpoint <= start or midpoint >= end:
        log.warning("%srange %s -> %s cannot be split further; taking as-is", indent, start, end)
        yield (start, end, count)
        return

    log.info("%s%s -> %s: %d events, splitting", indent, start.date(), end.date(), count)
    yield from plan_chunks(start, midpoint, depth + 1)
    yield from plan_chunks(midpoint, end, depth + 1)


def write_chunk(rows, path):
    columns = {name: [row.get(name) for row in rows] for name in usgs.COLUMNS}
    table = pa.table(columns, schema=SCHEMA)
    pq.write_table(table, path, compression="zstd")
    return table.num_rows


def load_manifest():
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text())
    return {"chunks": {}, "started_at": None, "finished_at": None}


def save_manifest(manifest):
    MANIFEST.write_text(json.dumps(manifest, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-year",
        type=int,
        default=1970,
        help="First year to seed. Pre-1970 coverage is sparse and instrument-poor.",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="ISO date to stop at (default: today at 00:00 UTC). The gap between "
        "this and the live feed is handled by the ingest job's backfill.",
    )
    parser.add_argument("--resume", action="store_true", help="Skip chunks already on disk.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    start = dt.datetime(args.start_year, 1, 1, tzinfo=dt.timezone.utc)
    if args.end:
        end = dt.datetime.fromisoformat(args.end).replace(tzinfo=dt.timezone.utc)
    else:
        end = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    manifest = load_manifest()
    manifest["started_at"] = manifest["started_at"] or dt.datetime.now(dt.timezone.utc).isoformat()
    manifest["seed_range"] = {"start": start.isoformat(), "end": end.isoformat()}

    log.info("seeding %s -> %s", start.date(), end.date())

    # Work a year at a time: plan that year, download it, then move on. Two
    # reasons. First, bisection never asks USGS for an unbounded count -- that
    # is exactly the request that 503s with "table is full". Second, downloads
    # start in seconds instead of after a full planning pass, and an interrupted
    # run resumes at year granularity.
    downloaded = 0
    chunk_index = 0
    cursor = start

    while cursor < end:
        year_end = min(dt.datetime(cursor.year + 1, 1, 1, tzinfo=dt.timezone.utc), end)
        log.info("=== %d ===", cursor.year)

        for chunk_start, chunk_end, expected in plan_chunks(cursor, year_end):
            chunk_index += 1
            path = RAW_DIR / chunk_name(chunk_start, chunk_end)
            key = path.name

            if args.resume and path.exists() and key in manifest["chunks"]:
                log.info("[%d] %s already on disk, skipping", chunk_index, key)
                downloaded += manifest["chunks"][key]["rows"]
                continue

            rows = usgs.fetch_range(chunk_start, chunk_end)
            written = write_chunk(rows, path)
            downloaded += written

            manifest["chunks"][key] = {
                "start": chunk_start.isoformat(),
                "end": chunk_end.isoformat(),
                "expected": expected,
                "rows": written,
                "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
            save_manifest(manifest)

            log.info(
                "[%d] %s: %d rows (expected %d) | running total %d",
                chunk_index,
                key,
                written,
                expected,
                downloaded,
            )
            time.sleep(REQUEST_DELAY_SECONDS)

        cursor = year_end

    manifest["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest["total_rows"] = downloaded
    save_manifest(manifest)

    size_mb = sum(p.stat().st_size for p in RAW_DIR.glob("*.parquet")) / 1_048_576
    log.info("DONE: %d events across %d files, %.1f MB on disk", downloaded, chunk_index, size_mb)


if __name__ == "__main__":
    main()
