"""Bulk-load the downloaded history into BigQuery. Reads only from disk.

This is the *other* compute path: a one-time bulk load, deliberately not the
same tool as the 15-minute job. It runs on a laptop, takes minutes, and moves
millions of rows; the ingest job runs on a hosted runner, takes seconds, and
moves hundreds. Sizing them the same would be wrong in both directions.

    python -m seed.load_seed              # consolidate, load, merge
    python -m seed.load_seed --skip-consolidate

Requires seed/download_history.py to have run first.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import bq, config  # noqa: E402

log = logging.getLogger("load_seed")

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
STAGED_DIR = ROOT / "data" / "staged"

# Consolidate chunks into files of roughly this many rows before loading.
# ~350 tiny load jobs is minutes of pure per-job overhead; a handful of larger
# files is one of the oldest performance rules in data engineering.
ROWS_PER_STAGED_FILE = 500_000

SEED_RAW_TABLE = "events_seed_raw"


def consolidate():
    """Merge the download chunks into a few large parquet files."""
    STAGED_DIR.mkdir(parents=True, exist_ok=True)
    for stale in STAGED_DIR.glob("*.parquet"):
        stale.unlink()

    chunks = sorted(RAW_DIR.glob("*.parquet"))
    if not chunks:
        raise SystemExit(f"no parquet chunks in {RAW_DIR} -- run seed.download_history first")

    log.info("consolidating %d chunks", len(chunks))

    staged = []
    batch = []
    batch_rows = 0

    def flush():
        nonlocal batch, batch_rows
        if not batch:
            return
        path = STAGED_DIR / f"seed_{len(staged):03d}.parquet"
        pq.write_table(pa.concat_tables(batch), path, compression="zstd")
        staged.append(path)
        log.info("  wrote %s (%d rows, %.1f MB)", path.name, batch_rows, path.stat().st_size / 1e6)
        batch = []
        batch_rows = 0

    for chunk in chunks:
        table = pq.read_table(chunk)
        batch.append(table)
        batch_rows += table.num_rows
        if batch_rows >= ROWS_PER_STAGED_FILE:
            flush()
    flush()

    total = sum(pq.read_metadata(p).num_rows for p in staged)
    log.info("consolidated to %d files, %d rows", len(staged), total)
    return staged


def load_to_raw_table(client, staged):
    """Load consolidated parquet into a scratch table."""
    target = config.table(SEED_RAW_TABLE)

    for index, path in enumerate(staged):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if index == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
        )
        started = time.time()
        with open(path, "rb") as handle:
            job = client.load_table_from_file(handle, target, job_config=job_config)
        job.result()
        log.info(
            "  loaded %s -> %s (%d rows, %.1fs)",
            path.name,
            SEED_RAW_TABLE,
            job.output_rows,
            time.time() - started,
        )

    count = list(client.query(f"SELECT COUNT(*) AS n FROM `{target}`").result())[0].n
    log.info("%s holds %d rows", SEED_RAW_TABLE, count)
    return count


def merge_into_events(client):
    """Dedupe the seed and MERGE it into events.

    Two layers of dedup, because the seed has two independent ways to repeat an
    id:

    1. QUALIFY ROW_NUMBER() ... ORDER BY updated DESC -- the arg_max pattern.
       FDSN start/end times are inclusive on both ends, so an event landing
       exactly on a chunk boundary appears in two files.
    2. MERGE rather than INSERT -- so loading the seed after the live job has
       already started (or re-running the seed entirely) converges instead of
       duplicating. Bulk load and trickle are not required to happen in a
       particular order, which is one less thing to get wrong at 2 a.m.
    """
    events = config.table("events")
    raw = config.table(SEED_RAW_TABLE)

    merge_sql = f"""
        MERGE `{events}` AS T
        USING (
          SELECT * FROM `{raw}`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated DESC, ingested_at DESC) = 1
        ) AS S
        ON T.id = S.id
        WHEN MATCHED AND S.updated > T.updated THEN UPDATE SET
          event_time = S.event_time, updated = S.updated, mag = S.mag,
          mag_type = S.mag_type, place = S.place, latitude = S.latitude,
          longitude = S.longitude, depth_km = S.depth_km, status = S.status,
          event_type = S.event_type, net = S.net, tsunami = S.tsunami,
          sig = S.sig, felt = S.felt, cdi = S.cdi, mmi = S.mmi, alert = S.alert,
          nst = S.nst, dmin = S.dmin, rms = S.rms, gap = S.gap, url = S.url,
          source = S.source, ingested_at = S.ingested_at
        WHEN NOT MATCHED THEN INSERT ROW
    """
    started = time.time()
    job = client.query(merge_sql)
    job.result()
    log.info(
        "merged in %.1fs (%s rows affected, %.2f GB processed)",
        time.time() - started,
        job.num_dml_affected_rows,
        (job.total_bytes_processed or 0) / 1e9,
    )
    return job.num_dml_affected_rows


def verify(client):
    events = config.table("events")
    row = list(
        client.query(
            f"""
            SELECT
              COUNT(*)                AS total_rows,
              COUNT(DISTINCT id)      AS distinct_ids,
              MIN(event_time)         AS earliest,
              MAX(event_time)         AS latest
            FROM `{events}`
            """
        ).result()
    )[0]

    log.info("events: %d rows, %d distinct ids", row.total_rows, row.distinct_ids)
    log.info("range:  %s -> %s", row.earliest, row.latest)

    if row.total_rows != row.distinct_ids:
        log.error("DUPLICATE IDS: %d rows but %d ids", row.total_rows, row.distinct_ids)
        return False
    log.info("no duplicate ids -- idempotency holds")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-consolidate", action="store_true", help="Reuse data/staged/.")
    parser.add_argument("--keep-raw-table", action="store_true", help="Leave the scratch table.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"
    )

    started_at = dt.datetime.now(dt.timezone.utc)
    client = bq.client()

    if args.skip_consolidate:
        staged = sorted(STAGED_DIR.glob("*.parquet"))
        log.info("reusing %d staged files", len(staged))
    else:
        staged = consolidate()

    fetched = load_to_raw_table(client, staged)
    affected = merge_into_events(client)
    ok = verify(client)

    if not args.keep_raw_table:
        client.query(f"DROP TABLE IF EXISTS `{config.table(SEED_RAW_TABLE)}`").result()
        log.info("dropped scratch table %s", SEED_RAW_TABLE)

    finished_at = dt.datetime.now(dt.timezone.utc)
    bq.record_run(
        client,
        {
            "run_id": f"seed-{started_at:%Y%m%dT%H%M%S}",
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
            "trigger": "manual",
            "mode": "seed",
            "window_start": None,
            "window_end": None,
            "rows_fetched": fetched,
            "rows_inserted": affected,
            "rows_updated": 0,
            "status": "ok" if ok else "error",
            "error_message": None if ok else "duplicate ids after seed merge",
            "runner_url": None,
        },
    )

    log.info("seed load complete in %s", finished_at - started_at)


if __name__ == "__main__":
    main()
