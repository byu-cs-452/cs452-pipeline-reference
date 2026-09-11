-- Canonical DDL for the pipeline. Applied by `python -m pipeline.bootstrap`.
-- Idempotent: safe to re-run. {dataset} is substituted at runtime.

-- ---------------------------------------------------------------------------
-- events: one row per earthquake, the current best revision of it.
--
-- PARTITION BY DATE(event_time): every analytical query in sql/ filters or
--   groups by event time, and the MERGE prunes to the handful of partitions the
--   incoming batch touches instead of rewriting a 4M-row table.
-- CLUSTER BY id: the hot path is a point-lookup MERGE on id, 96 times a day.
--   High-cardinality string clustering is exactly the case clustering is for.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{dataset}.events`
(
  id          STRING    NOT NULL OPTIONS(description="USGS event id, e.g. nc75433607. Primary key."),
  event_time  TIMESTAMP NOT NULL OPTIONS(description="When the quake happened (UTC)."),
  updated     TIMESTAMP          OPTIONS(description="When USGS last revised this event. Drives the MERGE: a row is replaced only by a strictly newer revision."),
  mag         FLOAT64            OPTIONS(description="Magnitude. Revised upward/downward as analysts review."),
  mag_type    STRING             OPTIONS(description="Magnitude scale: ml, mb, mww, ..."),
  place       STRING             OPTIONS(description="Human-readable location."),
  latitude    FLOAT64,
  longitude   FLOAT64,
  depth_km    FLOAT64,
  status      STRING             OPTIONS(description="'automatic' (machine-picked) or 'reviewed' (human-confirmed). Flips over time."),
  event_type  STRING             OPTIONS(description="earthquake, quarry blast, explosion, ice quake, ..."),
  net         STRING             OPTIONS(description="Contributing seismic network."),
  tsunami     BOOL,
  sig         INT64              OPTIONS(description="USGS significance score."),
  felt        INT64              OPTIONS(description="Count of 'Did You Feel It?' reports."),
  cdi         FLOAT64,
  mmi         FLOAT64,
  alert       STRING,
  nst         INT64,
  dmin        FLOAT64,
  rms         FLOAT64,
  gap         FLOAT64,
  url         STRING,
  source      STRING             OPTIONS(description="How this row arrived: 'seed' (bulk history), 'feed' (live poll), or 'backfill' (gap repair). Lets us prove the trickle is real and not just the seed."),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="When OUR pipeline wrote this row. The liveness proof lives in this column.")
)
PARTITION BY DATE(event_time)
CLUSTER BY id
OPTIONS(description="USGS earthquake catalog. Seeded from FDSN history, kept current by a 15-minute GitHub Actions job.");


-- ---------------------------------------------------------------------------
-- events_staging: landing zone for one ingest run.
--
-- Every run truncates and rewrites this. Loading here first, then MERGEing, is
-- what makes a run atomic from the reader's point of view -- `events` is never
-- half-updated. Unpartitioned because it holds at most a few thousand rows.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{dataset}.events_staging`
(
  id          STRING    NOT NULL,
  event_time  TIMESTAMP NOT NULL,
  updated     TIMESTAMP,
  mag         FLOAT64,
  mag_type    STRING,
  place       STRING,
  latitude    FLOAT64,
  longitude   FLOAT64,
  depth_km    FLOAT64,
  status      STRING,
  event_type  STRING,
  net         STRING,
  tsunami     BOOL,
  sig         INT64,
  felt        INT64,
  cdi         FLOAT64,
  mmi         FLOAT64,
  alert       STRING,
  nst         INT64,
  dmin        FLOAT64,
  rms         FLOAT64,
  gap         FLOAT64,
  url         STRING,
  source      STRING,
  ingested_at TIMESTAMP NOT NULL
)
OPTIONS(description="Transient landing table, truncated at the start of every ingest run.");


-- ---------------------------------------------------------------------------
-- ingest_runs: an audit row for EVERY run, including the ones that did nothing.
--
-- This is the table that answers the follow-up assignment. "Prove there were no
-- gaps" is not something you can answer from `events` alone -- a quiet hour and
-- a dead pipeline look identical there. A run that fetched zero new events still
-- writes a row here, so silence and failure are distinguishable after the fact.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `{dataset}.ingest_runs`
(
  run_id          STRING    NOT NULL OPTIONS(description="Unique per run; the GitHub Actions run id when scheduled."),
  started_at      TIMESTAMP NOT NULL,
  finished_at     TIMESTAMP,
  duration_ms     INT64,
  trigger         STRING             OPTIONS(description="'schedule', 'manual', or 'local'."),
  mode            STRING             OPTIONS(description="'feed' for the normal 24h overlap poll, 'backfill' when repairing a detected gap."),
  window_start    TIMESTAMP          OPTIONS(description="Start of the time range this run asked USGS for (backfill mode)."),
  window_end      TIMESTAMP,
  rows_fetched    INT64              OPTIONS(description="Rows USGS returned, before dedup against what we already had."),
  rows_inserted   INT64              OPTIONS(description="Genuinely new events."),
  rows_updated    INT64              OPTIONS(description="Existing events whose revision was newer than ours."),
  status          STRING    NOT NULL OPTIONS(description="'ok', 'no_new_data', or 'error'."),
  error_message   STRING,
  runner_url      STRING             OPTIONS(description="Link to the GitHub Actions run, for post-mortems.")
)
PARTITION BY DATE(started_at)
OPTIONS(description="One row per ingest attempt. The continuity ledger.");
