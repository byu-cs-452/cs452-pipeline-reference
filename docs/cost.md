# Cost accounting

**Actual spend: $0.00.**

Verified in the [GCP billing console](https://console.cloud.google.com/billing) for
project `cs452-508317` and the GitHub Actions usage page. Everything below sits inside a
permanent free tier — not a trial, not credits.

## Breakdown

| Resource | Usage | Free allowance | Cost |
|---|---|---|---|
| BigQuery storage | 1.118 GB (4.84M rows, active) | 10 GB/month | $0 |
| BigQuery queries | ~2 GB/month scanned | 1 TB/month | $0 |
| BigQuery load jobs | ~96/day + ~10 seed | unlimited, free | $0 |
| BigQuery streaming inserts | **none — deliberately** | n/a (billed from row 1) | $0 |
| GitHub Actions | ~96 runs/day × ~30s ≈ 50 min/day | unlimited (public) / 2,000 min/mo (private) | $0 |
| USGS FDSN + feeds | ~100 seed requests, 96 feed polls/day | free public service | $0 |
| Cloud Run job | ~96 executions/day × ~9s, 512 MiB | 180k vCPU-s + 360k GiB-s/month | $0 |
| Cloud Scheduler | 1 job | 3 jobs/month free | $0 |
| Artifact Registry | 1 image, ~250 MB | 0.5 GB/month free | $0 |
| Cloud Build | ~1 build | 2,500 build-min/month free | $0 |
| Cloud Storage | none — parquet stays local, loads go direct | n/a | $0 |
| Workload Identity Federation | ~96 token exchanges/day | free | $0 |

### The one that would have cost money

`ingest_runs` is written with **load jobs, not streaming inserts**. Load jobs are free
and unlimited; streaming is billed per 200 MB from the very first row. At 96 rows a day
the bill would have been trivial — a few cents a year — but the habit is the point. The
BigQuery Python client makes `insert_rows_json` the obvious call and it quietly puts you
on the metered path; `load_table_from_json` is barely more code and is free. On a table
that gets 96 rows a day it doesn't matter. On an event table at production volume, that
one-line difference is the entire bill.

### Why partition pruning matters more than the number suggests

The MERGE runs 96 times a day against a 4.8M-row table. Without the partition predicate
each one would scan all 681 partitions:

- **With pruning:** ~5 MB scanned per run → ~15 GB/month → free.
- **Without pruning:** ~1.2 GB per run → ~3.5 TB/month → ~2.5 TB over the free tier ≈
  **$16/month**.

Still small, because the table is small. Scale the same mistake to a 5 TB table and it's
roughly $600/month for a predicate you forgot to write. The habit is cheap to build here
and expensive to learn later.

## Sandbox vs. billed project

This project has **billing enabled**, so the free tier is headroom rather than a wall.
The distinction matters more than it sounds:

- **BigQuery sandbox** (no credit card): hard caps. Exceed 10 GB storage or 1 TB of
  queries and it *refuses*. Tables also auto-expire after 60 days — which for this
  assignment's one-week follow-up would be fine, but is worth knowing before you find
  out.
- **Billed project:** the same free tier, but exceeding it silently succeeds and charges
  you.

For coursework the sandbox's refusal is arguably the safer failure mode: a query that
won't run is a much better teacher than an invoice. The 60-day table expiry is the catch
— set explicit expirations or be surprised.

## Capacity math for the seed

Done before committing to a scope, which is the point of the exercise:

- ~4.84M events × ~24 columns, mostly nullable floats and short strings
- ≈ 230 bytes/row uncompressed → **1.118 GB in BigQuery** (measured, not estimated)
- ≈ 58 bytes/row as zstd parquet → **270 MB on local disk** across 389 chunks
- Growth: ~600 new events/day ≈ 0.2 MB/day ≈ **65 MB/year**

Comfortably inside 10 GB, with decades of headroom. Had this been the Citi Bike
challenge option — 100M+ trips, much wider rows — the seed would have needed trimming to
fit, which is exactly the capacity-planning decision the assignment asks you to document.

## Teardown

Nothing here bills by the hour, so there is no meter running and no urgency to delete.
Contrast with Azure Event Hubs, which bills per namespace-hour whether or not anything
flows through it — the assignment's "delete the namespace when done" warning has no
equivalent on this stack. Teardown steps are in [`runbook.md`](runbook.md); the reason
to run them is API politeness, not cost.
