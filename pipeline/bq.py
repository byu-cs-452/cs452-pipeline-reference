"""BigQuery helpers: schema bootstrap, staging loads, and the idempotent MERGE."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

from google.cloud import bigquery

from pipeline import config, usgs

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# Explicit schema for staging loads. Letting BigQuery autodetect from JSON is how
# you end up with a column that is INT64 on a quiet day and FLOAT64 on a busy one.
STAGING_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("updated", "TIMESTAMP"),
    bigquery.SchemaField("mag", "FLOAT64"),
    bigquery.SchemaField("mag_type", "STRING"),
    bigquery.SchemaField("place", "STRING"),
    bigquery.SchemaField("latitude", "FLOAT64"),
    bigquery.SchemaField("longitude", "FLOAT64"),
    bigquery.SchemaField("depth_km", "FLOAT64"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("net", "STRING"),
    bigquery.SchemaField("tsunami", "BOOL"),
    bigquery.SchemaField("sig", "INT64"),
    bigquery.SchemaField("felt", "INT64"),
    bigquery.SchemaField("cdi", "FLOAT64"),
    bigquery.SchemaField("mmi", "FLOAT64"),
    bigquery.SchemaField("alert", "STRING"),
    bigquery.SchemaField("nst", "INT64"),
    bigquery.SchemaField("dmin", "FLOAT64"),
    bigquery.SchemaField("rms", "FLOAT64"),
    bigquery.SchemaField("gap", "FLOAT64"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]


def client():
    """Application Default Credentials.

    Locally that is your gcloud login. In GitHub Actions it is the short-lived
    token minted by Workload Identity Federation -- google-auth finds it the same
    way either place, so this code needs no environment-specific branch.
    """
    return bigquery.Client(project=config.GCP_PROJECT, location=config.BQ_LOCATION)


def ensure_dataset(bq):
    dataset = bigquery.Dataset(config.dataset_ref())
    dataset.location = config.BQ_LOCATION
    dataset.description = "USGS earthquake pipeline (BYU CS 452 reference solution)."
    bq.create_dataset(dataset, exists_ok=True)
    log.info("dataset ready: %s (%s)", config.dataset_ref(), config.BQ_LOCATION)


def apply_schema(bq):
    """Run sql/schema.sql. Every statement is CREATE TABLE IF NOT EXISTS."""
    ddl = (SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    ddl = ddl.replace("{dataset}", config.dataset_ref())

    for statement in _split_statements(ddl):
        bq.query(statement).result()
    log.info("schema applied")


def _split_statements(sql_text):
    """Split on statement-terminating semicolons.

    Quote- and comment-aware, because `sql_text.split(";")` is not: a column
    description reading "Unique per run; the Actions run id" gets sliced in half
    and BigQuery answers with "Unclosed string literal", which points at the
    column rather than at the splitter. Tracking whether we are inside a quote
    or a `--` comment is a dozen lines and removes a whole class of confusing
    failures.
    """
    statements = []
    current = []
    quote = None
    in_comment = False
    index = 0

    while index < len(sql_text):
        char = sql_text[index]

        if in_comment:
            if char == "\n":
                in_comment = False
                current.append(char)
            index += 1
            continue

        if quote:
            current.append(char)
            if char == "\\":  # escaped char inside a literal; take the next one as-is
                if index + 1 < len(sql_text):
                    current.append(sql_text[index + 1])
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue

        if char in ("'", '"', "`"):
            quote = char
            current.append(char)
        elif char == "-" and sql_text[index : index + 2] == "--":
            in_comment = True
            index += 2
            continue
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)

        index += 1

    statements.append("".join(current))

    for statement in statements:
        if statement.strip():
            yield statement.strip()


def load_rows_to_staging(bq, rows):
    """Truncate-and-load this run's rows into events_staging."""
    if not rows:
        return 0

    payload = [_jsonify(row) for row in rows]
    job_config = bigquery.LoadJobConfig(
        schema=STAGING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq.load_table_from_json(payload, config.table("events_staging"), job_config=job_config)
    job.result()
    log.info("staged %d rows", len(payload))
    return len(payload)


def merge_staging_into_events(bq):
    """The idempotency core.

    Two things make re-running safe:

    1. MATCHED requires `S.updated > T.updated`. Re-ingesting the same event with
       the same revision is a no-op, so the 24-hour overlap window costs nothing.
       Re-ingesting a *newer* revision -- a magnitude corrected from 4.1 to 4.4,
       or status flipping automatic -> reviewed -- replaces the row. USGS really
       does this: a 2026-01-01 event in our seed carries an `updated` of
       2026-05-14.
    2. The target is pruned to the partitions the batch actually touches. Without
       that predicate BigQuery scans all 50+ years of partitions on every run,
       96 times a day, to apply a few hundred rows.

    Returns (inserted, updated). BigQuery reports only a combined DML row count,
    so the two are counted separately before the MERGE runs.
    """
    events = config.table("events")
    staging = config.table("events_staging")

    # One probe query does double duty: it counts what the MERGE is about to do,
    # and it computes the partition window the MERGE needs. The window has to be
    # resolved here rather than inline, because BigQuery rejects a subquery that
    # references a table inside a MERGE join predicate:
    #   "Unsupported subquery with table in join predicate."
    # So the bounds go in as query parameters, which are plain constants.
    probe = list(
        bq.query(
            f"""
            SELECT
              COUNTIF(T.id IS NULL)                               AS to_insert,
              COUNTIF(T.id IS NOT NULL AND S.updated > T.updated) AS to_update,
              MIN(DATE(S.event_time))                             AS min_day,
              MAX(DATE(S.event_time))                             AS max_day
            FROM `{staging}` AS S
            LEFT JOIN `{events}` AS T USING (id)
            """
        ).result()
    )[0]

    if probe.min_day is None:
        log.info("staging is empty, nothing to merge")
        return 0, 0

    merge_sql = f"""
        MERGE `{events}` AS T
        USING `{staging}` AS S
        ON T.id = S.id
           -- Partition pruning: only touch the days this batch covers, padded by
           -- a day on each side.
           --
           -- The padding is not cosmetic. A predicate in the ON clause that
           -- wrongly excludes an existing row does not just skip the update --
           -- the row falls through to NOT MATCHED and gets INSERTED, duplicating
           -- the id. BigQuery enforces no primary key, so nothing would stop it.
           -- A relocated event's origin time can shift by a second or two, which
           -- is enough to cross midnight into an unscanned partition. One day of
           -- slack costs two extra partitions out of ~20,000 and removes the
           -- failure mode entirely.
           AND DATE(T.event_time) BETWEEN @min_day AND @max_day
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

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "min_day", "DATE", probe.min_day - dt.timedelta(days=1)
            ),
            bigquery.ScalarQueryParameter(
                "max_day", "DATE", probe.max_day + dt.timedelta(days=1)
            ),
        ]
    )
    bq.query(merge_sql, job_config=job_config).result()

    inserted = probe.to_insert or 0
    updated = probe.to_update or 0
    log.info("merge complete: %d inserted, %d updated", inserted, updated)
    return inserted, updated


def latest_event_time(bq):
    """Newest event we hold. Drives the gap check -- None means an empty table."""
    rows = list(bq.query(f"SELECT MAX(event_time) AS t FROM `{config.table('events')}`").result())
    return rows[0].t if rows else None


def record_run(bq, record):
    """Append one row to ingest_runs.

    Deliberately uses a load job, not streaming inserts: load jobs are free,
    streaming is not, and this table gets 96 rows a day forever.
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("finished_at", "TIMESTAMP"),
            bigquery.SchemaField("duration_ms", "INT64"),
            bigquery.SchemaField("trigger", "STRING"),
            bigquery.SchemaField("mode", "STRING"),
            bigquery.SchemaField("window_start", "TIMESTAMP"),
            bigquery.SchemaField("window_end", "TIMESTAMP"),
            bigquery.SchemaField("rows_fetched", "INT64"),
            bigquery.SchemaField("rows_inserted", "INT64"),
            bigquery.SchemaField("rows_updated", "INT64"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("runner_url", "STRING"),
        ],
    )
    job = bq.load_table_from_json(
        [_jsonify(record)], config.table("ingest_runs"), job_config=job_config
    )
    job.result()


def _jsonify(row):
    """datetimes -> RFC3339 strings, which is what a JSON load job expects."""
    out = {}
    for key, value in row.items():
        if isinstance(value, dt.datetime):
            out[key] = value.astimezone(dt.timezone.utc).isoformat()
        else:
            out[key] = value
    return out


def column_order_matches_schema():
    """Guard: sql/schema.sql, usgs.COLUMNS and STAGING_SCHEMA must agree.

    MERGE's `INSERT ROW` inserts staging columns positionally. If the three
    definitions ever drift, that silently writes longitude into latitude. Cheap
    assertion, catastrophic bug prevented -- see tests/test_schema_alignment.py.
    """
    return [field.name for field in STAGING_SCHEMA] == list(usgs.COLUMNS)
