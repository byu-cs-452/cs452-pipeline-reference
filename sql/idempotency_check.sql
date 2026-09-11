-- Proof that re-fetching overlapping data does not duplicate or lose anything.
--
-- The pipeline re-reads the same 24-hour window 96 times a day. Every event is
-- therefore fetched ~96 times. These queries show what that produces.

-- ---------------------------------------------------------------------------
-- 1. The assertion that matters. Must return zero rows, always.
--    If MERGE ever mismatches -- a bad ON clause, a partition filter excluding a
--    row that then falls through to INSERT -- this is where it shows up.
-- ---------------------------------------------------------------------------
SELECT id, COUNT(*) AS copies
FROM `cs393-496021.usgs_pipeline.events`
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY copies DESC
LIMIT 20;


-- ---------------------------------------------------------------------------
-- 2. Same thing as one comparable pair, easier to screenshot.
-- ---------------------------------------------------------------------------
SELECT
  COUNT(*)                              AS total_rows,
  COUNT(DISTINCT id)                    AS distinct_ids,
  COUNT(*) - COUNT(DISTINCT id)         AS duplicates
FROM `cs393-496021.usgs_pipeline.events`;


-- ---------------------------------------------------------------------------
-- 3. Why `updated` exists, and why dedup-on-id alone would be wrong.
--
--    USGS revises events after the fact: an automatic magnitude gets reviewed
--    and corrected, sometimes months later. A naive "INSERT ... IF NOT EXISTS"
--    would pin the first guess forever and silently serve stale magnitudes. The
--    MERGE replaces a row only when a strictly newer revision arrives, so these
--    corrections land.
-- ---------------------------------------------------------------------------
SELECT
  status,
  COUNT(*)                                                          AS events,
  ROUND(AVG(TIMESTAMP_DIFF(updated, event_time, HOUR)), 1)          AS avg_hours_to_last_revision,
  MAX(TIMESTAMP_DIFF(updated, event_time, DAY))                     AS max_days_to_last_revision
FROM `cs393-496021.usgs_pipeline.events`
WHERE updated IS NOT NULL
GROUP BY status
ORDER BY events DESC;


-- ---------------------------------------------------------------------------
-- 4. Events this pipeline actually revised after first ingesting them --
--    rows_updated > 0 runs. Direct evidence the overlap window earns its keep.
-- ---------------------------------------------------------------------------
SELECT
  started_at,
  mode,
  rows_inserted,
  rows_updated
FROM `cs393-496021.usgs_pipeline.ingest_runs`
WHERE rows_updated > 0
ORDER BY started_at DESC
LIMIT 25;
