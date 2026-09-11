-- Is the pipeline alive? Charted in Looker Studio; also runnable in the console.
--
-- The key column is ingested_at, NOT event_time. event_time tells you when the
-- earth moved; ingested_at tells you when OUR pipeline was last awake. Only the
-- second one can prove nobody is pushing the button.

-- ---------------------------------------------------------------------------
-- 1. Arrivals per hour over the last 3 days, split by how they got here.
--    A healthy pipeline shows an unbroken run of 'feed' bars.
-- ---------------------------------------------------------------------------
SELECT
  TIMESTAMP_TRUNC(ingested_at, HOUR) AS ingest_hour,
  source,
  COUNT(*)                           AS events_written,
  ROUND(MAX(mag), 1)                 AS biggest_mag
FROM `cs452-508317.usgs_pipeline.events`
WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
GROUP BY ingest_hour, source
ORDER BY ingest_hour DESC, source;


-- ---------------------------------------------------------------------------
-- 2. Freshness, one row. This is the number to put on a dashboard tile.
-- ---------------------------------------------------------------------------
SELECT
  MAX(ingested_at)                                                    AS last_write,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), MINUTE)       AS minutes_since_write,
  MAX(event_time)                                                     AS newest_quake,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_time), MINUTE)        AS minutes_behind_reality,
  COUNT(*)                                                            AS total_events
FROM `cs452-508317.usgs_pipeline.events`;


-- ---------------------------------------------------------------------------
-- 3. The ten most recent events we have, with how long WE took to pick each up.
--    Latency here is USGS publishing lag plus up to one 15-minute poll interval.
-- ---------------------------------------------------------------------------
SELECT
  id,
  event_time,
  ROUND(mag, 1)  AS mag,
  place,
  status,
  ingested_at,
  TIMESTAMP_DIFF(ingested_at, event_time, MINUTE) AS pickup_lag_minutes
FROM `cs452-508317.usgs_pipeline.events`
WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
ORDER BY event_time DESC
LIMIT 10;
