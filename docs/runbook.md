# Runbook — how this breaks, and what to do

Written before anything broke, which is the only time you can think clearly about it.
For the one-week check-in, start at [Is it alive?](#is-it-alive) and work down.

## Is it alive?

Three checks, in order of how much they tell you:

1. **Freshness** — query 2 in [`../sql/liveness.sql`](../sql/liveness.sql).
   `minutes_since_write` under ~20 means healthy. Over 60 means something is wrong.
2. **Continuity** — query 1 in [`../sql/continuity.sql`](../sql/continuity.sql).
   Every run, with the delay since the previous one. Gaps are labeled.
3. **The Actions tab** — red X's are the runs that failed loudly. Note that a *missing*
   run leaves no X at all, which is why check 2 exists.

`events` alone cannot answer this. A quiet 40 minutes and a dead job both look like
"no new rows." Only `ingest_runs` distinguishes them.

## Failure modes

### GitHub disables the schedule after 60 days of inactivity
**Symptom:** runs simply stop; no failures, no emails, nothing in `ingest_runs`.
**Why:** GitHub's policy for scheduled workflows in inactive repos.
**Detect:** continuity query shows a gap with no error rows on either side.
**Fix:** push any commit, or re-enable in the Actions tab. Prevent with a dated
keepalive commit. *This is the single most likely cause of a quietly dead pipeline
after a semester break.*

### USGS returns 503
**Symptom:** `status='error'` in `ingest_runs`, `UsgsUnavailable` in `error_message`.
**Why:** their service overloads, particularly on large queries.
**Fix:** none needed. `_get` already retried 5 times with backoff; the next scheduled
run re-reads the same 24-hour window and closes the hole. Only act if errors persist
past ~4 consecutive runs.

### Unbounded FDSN count returns "table is full"
**Symptom:** `503 ... General error: 1114 The table '/rdsdbdata/tmp/#sql...' is full`.
**Why:** USGS's backend cannot scan the entire catalog for one query. Hit during this
build on `/count?starttime=1900-01-01`.
**Fix:** already designed around — the seeder bisects time ranges and never asks an
unbounded question. If you extend the seeder, keep that property.

### Gap between seed and live data
**Symptom:** a hole between the seed's end and the first scheduled run.
**Fix:** automatic. `decide_mode` detects >20h staleness and escalates to an FDSN range
query. Verify by looking for `mode='backfill'` rows in `ingest_runs`.

### Outage longer than 24 hours
**Symptom:** feed window can no longer cover the hole.
**Fix:** automatic, same path — but capped at `MAX_BACKFILL_DAYS` (30) per run, so a very
long outage repairs over several runs rather than one enormous request to a free service.

### Duplicate ids appear
**Symptom:** query 1 in [`../sql/idempotency_check.sql`](../sql/idempotency_check.sql)
returns rows. Should never happen.
**Why:** almost certainly the MERGE `ON` clause — a partition predicate that excludes an
existing target row sends it to `NOT MATCHED` and inserts a copy. See the README's
"subtle one" callout.
**Fix:** dedupe with `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated DESC)
= 1`, then fix the predicate before re-running.

### WIF auth fails
**Symptom:** `google-github-actions/auth` step fails; nothing reaches BigQuery.
**Check, in order:** `id-token: write` present in workflow permissions; the five repo
variables still set; the repo hasn't been renamed (the principalSet binding pins
`byu-cs-452/cs452-pipeline-reference` by name — **renaming the repo breaks auth**);
service account not deleted.

### Concurrency collision
**Symptom:** a run queues behind another.
**Why:** the `concurrency` group serializes runs on purpose — a delayed run overlapping
the next one must not MERGE simultaneously.
**Fix:** none. Working as designed.

## Teardown

When the follow-up assignment is done, in this order:

```bash
# 1. Stop the schedule first, so nothing writes to a half-deleted dataset.
gh workflow disable ingest --repo byu-cs-452/cs452-pipeline-reference

# 2. Drop the data.
gcloud alpha bq datasets delete usgs_pipeline --project=cs393-496021 --remove-tables

# 3. Remove the identity plumbing.
gcloud iam service-accounts delete usgs-pipeline-ingest@cs393-496021.iam.gserviceaccount.com --project=cs393-496021
gcloud iam workload-identity-pools delete github-pool --project=cs393-496021 --location=global
```

Nothing here bills by the hour, so there is no meter to stop — but leaving a job
hammering a free public API forever is how free public APIs disappear.
