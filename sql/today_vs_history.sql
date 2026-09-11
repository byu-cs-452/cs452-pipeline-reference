-- Joining the live trickle to the historical seed.
--
-- This is the query that justifies having both halves of the pipeline. Neither
-- side answers it alone: the seed knows what normal looks like but not what is
-- happening now, and the live feed knows what is happening now but has no idea
-- whether it is unusual.

-- ---------------------------------------------------------------------------
-- 1. Today vs. the same day-of-year across 50+ years of history.
--    "Is today a busy day for earthquakes?" -- answerable in one query.
-- ---------------------------------------------------------------------------
WITH today AS (
  SELECT
    COUNT(*)                                  AS events_today,
    COUNTIF(mag >= 4.5)                       AS m45_plus_today,
    ROUND(MAX(mag), 1)                        AS biggest_today
  FROM `cs393-496021.usgs_pipeline.events`
  WHERE DATE(event_time) = CURRENT_DATE()
),
historical_same_day AS (
  SELECT
    COUNT(*) / COUNT(DISTINCT EXTRACT(YEAR FROM event_time)) AS avg_events_per_day,
    COUNTIF(mag >= 4.5) / COUNT(DISTINCT EXTRACT(YEAR FROM event_time)) AS avg_m45_plus,
    ROUND(MAX(mag), 1)                                       AS biggest_ever_this_day,
    COUNT(DISTINCT EXTRACT(YEAR FROM event_time))            AS years_of_history
  FROM `cs393-496021.usgs_pipeline.events`
  WHERE EXTRACT(DAYOFYEAR FROM event_time) = EXTRACT(DAYOFYEAR FROM CURRENT_DATE())
    AND DATE(event_time) < CURRENT_DATE()
)
SELECT
  t.events_today,
  ROUND(h.avg_events_per_day, 1)                                AS avg_events_this_day_of_year,
  ROUND(t.events_today / NULLIF(h.avg_events_per_day, 0), 2)    AS ratio_vs_normal,
  t.m45_plus_today,
  ROUND(h.avg_m45_plus, 2)                                      AS avg_m45_plus,
  t.biggest_today,
  h.biggest_ever_this_day,
  h.years_of_history
FROM today AS t
CROSS JOIN historical_same_day AS h;


-- ---------------------------------------------------------------------------
-- 2. Detection improves over time. Counts by year and magnitude band show the
--    seismic network getting denser -- small quakes rise sharply, M5+ stays
--    roughly flat, because the earth did not change but our hearing did.
--    A good reminder that a rising line in a dataset is not always a rising
--    thing in the world.
-- ---------------------------------------------------------------------------
SELECT
  EXTRACT(YEAR FROM event_time) AS year,
  COUNTIF(mag <  2)             AS under_m2,
  COUNTIF(mag >= 2 AND mag < 4) AS m2_to_m4,
  COUNTIF(mag >= 4 AND mag < 5) AS m4_to_m5,
  COUNTIF(mag >= 5 AND mag < 6) AS m5_to_m6,
  COUNTIF(mag >= 6)             AS m6_plus,
  COUNT(*)                      AS total
FROM `cs393-496021.usgs_pipeline.events`
WHERE event_time < TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), YEAR)  -- drop the partial year
GROUP BY year
ORDER BY year;


-- ---------------------------------------------------------------------------
-- 3. The last 24 hours against the trailing 10-year hourly average.
--    Good dashboard line chart: actual vs. expected, hour by hour.
-- ---------------------------------------------------------------------------
WITH recent AS (
  SELECT
    TIMESTAMP_TRUNC(event_time, HOUR) AS hour,
    COUNT(*)                          AS events
  FROM `cs393-496021.usgs_pipeline.events`
  WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  GROUP BY hour
),
baseline AS (
  SELECT COUNT(*) / COUNT(DISTINCT TIMESTAMP_TRUNC(event_time, HOUR)) AS avg_events_per_hour
  FROM `cs393-496021.usgs_pipeline.events`
  WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3650 DAY)
    AND event_time <  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
)
SELECT
  r.hour,
  r.events                                              AS events_this_hour,
  ROUND(b.avg_events_per_hour, 1)                       AS ten_year_hourly_average,
  ROUND(r.events / NULLIF(b.avg_events_per_hour, 0), 2) AS ratio
FROM recent AS r
CROSS JOIN baseline AS b
ORDER BY r.hour DESC;
