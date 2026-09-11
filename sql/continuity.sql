-- Continuity: did it run every 15 minutes all week, and if not, when didn't it?
--
-- This is the follow-up assignment's real question, and it cannot be answered
-- from `events` alone. A quiet 40 minutes with no earthquakes and a dead job
-- look identical in `events` -- both are simply an absence of rows. The
-- ingest_runs ledger is what distinguishes them: a healthy-but-quiet run still
-- writes a row saying "I woke up, USGS had nothing new."

-- ---------------------------------------------------------------------------
-- 1. Gap hunt. Every run, with the delay since the previous one. Anything much
--    over the 15-minute cadence is a missed or delayed run -- point at it in the
--    video and explain it.
-- ---------------------------------------------------------------------------
WITH runs AS (
  SELECT
    run_id,
    started_at,
    status,
    mode,
    rows_inserted,
    rows_updated,
    error_message,
    LAG(started_at) OVER (ORDER BY started_at) AS previous_started_at
  FROM `cs452-508317.usgs_pipeline.ingest_runs`
  WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
)
SELECT
  started_at,
  status,
  mode,
  rows_inserted,
  rows_updated,
  TIMESTAMP_DIFF(started_at, previous_started_at, MINUTE) AS minutes_since_previous_run,
  CASE
    WHEN previous_started_at IS NULL                                   THEN 'first run in window'
    WHEN TIMESTAMP_DIFF(started_at, previous_started_at, MINUTE) <= 20 THEN 'on schedule'
    WHEN TIMESTAMP_DIFF(started_at, previous_started_at, MINUTE) <= 60 THEN 'late (GitHub queue delay)'
    ELSE 'GAP -- investigate'
  END AS verdict,
  error_message
FROM runs
ORDER BY started_at DESC;


-- ---------------------------------------------------------------------------
-- 2. Daily rollup: runs attempted vs expected (96/day at 15-minute cadence).
--    A clean week is 7 rows all showing close to 96.
-- ---------------------------------------------------------------------------
SELECT
  DATE(started_at)                        AS day,
  COUNT(*)                                AS runs,
  96                                      AS runs_expected,
  COUNTIF(status = 'ok')                  AS runs_with_new_data,
  COUNTIF(status = 'no_new_data')         AS runs_quiet,
  COUNTIF(status = 'error')               AS runs_failed,
  SUM(rows_inserted)                      AS events_inserted,
  SUM(rows_updated)                       AS events_revised,
  ROUND(AVG(duration_ms) / 1000, 1)       AS avg_seconds
FROM `cs452-508317.usgs_pipeline.ingest_runs`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
GROUP BY day
ORDER BY day DESC;


-- ---------------------------------------------------------------------------
-- 3. Every failure, newest first. Start a post-mortem here; runner_url goes
--    straight to the Actions log for that run.
-- ---------------------------------------------------------------------------
SELECT started_at, mode, error_message, runner_url
FROM `cs452-508317.usgs_pipeline.ingest_runs`
WHERE status = 'error'
ORDER BY started_at DESC
LIMIT 50;


-- ---------------------------------------------------------------------------
-- 4. Runs by scheduler. Two independent schedulers drive the same job, so this
--    is how you tell "one scheduler went quiet" apart from "the pipeline died".
--
--    During this build the github-schedule row sat at zero for over an hour
--    while cloud-run-scheduler ran normally -- a distinction invisible in
--    `events` and invisible in a ledger that does not record its own trigger.
-- ---------------------------------------------------------------------------
SELECT
  trigger,
  COUNT(*)                                                      AS runs,
  COUNTIF(status = 'error')                                     AS failures,
  MIN(started_at)                                               AS first_run,
  MAX(started_at)                                               AS last_run,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(started_at), MINUTE)  AS minutes_since_last,
  SUM(rows_inserted)                                            AS events_inserted,
  SUM(rows_updated)                                             AS events_revised
FROM `cs452-508317.usgs_pipeline.ingest_runs`
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
GROUP BY trigger
ORDER BY runs DESC;
