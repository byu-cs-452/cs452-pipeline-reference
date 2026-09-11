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

### GitHub's scheduled workflow never fires
**Symptom:** no scheduled runs at all, no failures, nothing in `ingest_runs` with a
`github-schedule` trigger. Manual `workflow_dispatch` runs work fine.
**Why:** GitHub schedules are best-effort. Observed during this build: a `*/15` cron on
an active workflow produced **zero** runs in over an hour. Worse at the top of the hour
and for newly-created schedules.
**Detect:** `SELECT trigger, COUNT(*) FROM ingest_runs WHERE started_at > ...
GROUP BY trigger` -- if `cloud-run-scheduler` rows are arriving and
`github-schedule` rows are not, GitHub is the problem, not the pipeline.
**Fix:** none available; it is not under your control. This is why Cloud Scheduler runs
the same job in parallel. If you rely on GitHub alone, budget for it being late or
absent and make sure your overlap window is wide enough to not care.

### Cloud Run execution starts minutes after the scheduler fires
**Symptom:** `lastAttemptTime` on the scheduler says 15:22, the `ingest_runs` row says
15:25. Observed repeatedly during this build, up to ~3 minutes.
**Why:** Cloud Run job provisioning -- pulling a ~250 MB image onto a cold instance. One
execution sat in `Waiting for execution to start` with
`System will retry after 00:05`, then ran normally on retry.
**Is it a problem?** No, and this is worth being precise about. The pipeline's
correctness does not depend on *when* a run happens, only that one happens within the
24-hour overlap window. A 3-minute start delay is invisible in the data. It would matter
only if you promised freshness tighter than a few minutes -- in which case a Cloud Run
*service* with a minimum instance, rather than a job, is the right shape.
**What did NOT fix it:** slimming the image. `pipeline.ingest` never imports pyarrow, so
the container now installs `requirements-ingest.txt` and drops ~100 MB of unused wheel.
Measured afterwards: trigger at 15:27:24, execution start at 15:29:46 -- about 2.5
minutes, essentially unchanged from before. **The delay is queueing and provisioning, not
image pull.** The slimmer image is still worth keeping (less to build, less to store,
less to audit), but do not expect it to buy latency. If you genuinely need sub-minute
starts, the answer is a Cloud Run *service* with `--min-instances=1`, not a job -- and
that leaves the free tier, so decide whether the freshness requirement is real first.

### Cloud Scheduler stops triggering
**Symptom:** no `cloud-run-scheduler` rows.
**Check:** `gcloud scheduler jobs describe usgs-ingest-every-15m --location us-central1`
-- confirm `state: ENABLED` and look at `status`. Then check the job's executions:
`gcloud run jobs executions list --job usgs-ingest --region us-central1`.
**Common causes:** the service account lost `roles/run.invoker` on the job, the Cloud Run
job was deleted or redeployed under a different name, or billing was disabled on the
project.

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
P=cs393-496021; R=us-central1

# 1. Stop BOTH schedules first, so nothing writes to a half-deleted dataset.
#    Missing either one leaves a job failing every 15 minutes forever.
gh workflow disable ingest --repo byu-cs-452/cs452-pipeline-reference
gh workflow disable healthcheck --repo byu-cs-452/cs452-pipeline-reference
gcloud scheduler jobs delete usgs-ingest-every-15m --project=$P --location=$R --quiet

# 2. Remove the compute.
gcloud run jobs delete usgs-ingest --project=$P --region=$R --quiet
gcloud artifacts repositories delete cloud-run-source-deploy --project=$P --location=$R --quiet

# 3. Drop the data.
gcloud alpha bq datasets delete usgs_pipeline --project=$P --remove-tables

# 4. Remove the identity plumbing.
gcloud iam service-accounts delete usgs-pipeline-ingest@$P.iam.gserviceaccount.com --project=$P
gcloud iam workload-identity-pools delete github-pool --project=$P --location=global
```

Verify nothing is still running afterwards:

```bash
gcloud scheduler jobs list --project=$P --location=$R
gh run list --repo byu-cs-452/cs452-pipeline-reference --limit 5
```

Nothing here bills by the hour, so there is no meter to stop — but leaving a job
hammering a free public API forever is how free public APIs disappear.
