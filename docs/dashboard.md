# Stage 5 — the dashboard

Looker Studio, free, with a native BigQuery connector. Setup is click-through (there is
no useful API for building reports), so it's written out here.

## Build it

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com) → **Create** →
   **Data source** → **BigQuery**.
2. Pick `cs393-496021` → `usgs_pipeline` → `events`. **Connect.**
3. Set `ingested_at` and `event_time` to type **Date & Time → Date Hour Minute**.
   Looker Studio defaults timestamps to plain Date, which flattens the whole liveness
   story into daily buckets.
4. Add `ingest_runs` as a second data source in the same report.

## The four tiles that make the case

### 1. "Last write" scorecard — the money tile
- Source: `events`, metric `ingested_at` → aggregation **Max**.
- Put it top-left at the largest size on the page. The demo video is 30 seconds; this is
  the thing the viewer should read first. Refresh it on camera and watch it move.

### 2. Arrivals per hour — the growth chart
- **Time series**, source `events`.
- Dimension: `ingested_at` (Date Hour). Metric: **Record Count**.
- Breakdown dimension: `source`. Filter to the last 3 days.
- This is the "it's alive" chart: an unbroken run of bars, each one a run nobody
  triggered. The `source` breakdown separates the one-time `seed` block from the ongoing
  `feed` trickle, which is what makes it *visibly* a pipeline and not a one-time import.

### 3. Run continuity — the honesty chart
- **Time series**, source `ingest_runs`.
- Dimension `started_at` (Date Hour), metric **Record Count**, breakdown `status`.
- Healthy is ~4 runs/hour in `ok`/`no_new_data`. Gaps and red `error` bars both show up
  here. **Do not hide this one** — the follow-up assignment explicitly rewards pointing
  at an anomaly and explaining it, and a chart with no gaps is only credible if it's the
  kind of chart that *would* show them.

### 4. Today vs. history — the join
- **Scorecard** or bar chart, from query 1 of
  [`../sql/today_vs_history.sql`](../sql/today_vs_history.sql) as a **custom query**
  data source.
- Shows today's count against the 50-year average for the same day-of-year. This is the
  tile that justifies having both halves of the pipeline: neither the seed nor the feed
  can answer it alone.

## Refresh

Looker Studio caches for 12 hours by default — which will make your live dashboard look
frozen during the demo. **File → Report settings → Data freshness → 15 minutes.**
Match the ingest cadence; anything faster just re-queries for no new data.

## A note on the demo video

The assignment asks for 30 seconds proving fresh data arrived unattended. The most
convincing version is not the dashboard alone — it's the pairing:

1. The **Actions tab**, showing a run that fired on schedule with nobody present.
2. The **last-write scorecard**, showing a timestamp from minutes ago.
3. The **continuity chart**, showing that pattern repeating for days.

One of those could be staged by hand. All three together can't be.
