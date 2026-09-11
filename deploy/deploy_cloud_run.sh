#!/usr/bin/env bash
# Stage 2, Google path: Cloud Scheduler -> Cloud Run job -> BigQuery.
#
# Runs the same pipeline.ingest module GitHub Actions runs. Idempotent -- safe to
# re-run to redeploy a new image.
#
#   bash deploy/deploy_cloud_run.sh
set -euo pipefail

PROJECT="${GCP_PROJECT:-cs393-496021}"
REGION="${CLOUD_RUN_REGION:-us-central1}"
DATASET="${BQ_DATASET:-usgs_pipeline}"
JOB="usgs-ingest"
SCHEDULER_JOB="usgs-ingest-every-15m"
SA="usgs-pipeline-ingest@${PROJECT}.iam.gserviceaccount.com"

echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com cloudscheduler.googleapis.com \
  --project "$PROJECT"

echo "==> Building and deploying the Cloud Run job"
# Note: no key material anywhere. The job *runs as* the service account, so the
# metadata server hands it credentials directly. Inside GCP there is no trust
# boundary to bridge, so there is nothing to federate and nothing to store --
# compare the GitHub path, which needs Workload Identity Federation precisely
# because the compute lives in someone else's trust domain.
gcloud run jobs deploy "$JOB" \
  --source . \
  --region "$REGION" \
  --project "$PROJECT" \
  --service-account "$SA" \
  --set-env-vars "GCP_PROJECT=${PROJECT},BQ_DATASET=${DATASET},BQ_LOCATION=US,CLOUD_RUN_REGION=${REGION}" \
  --max-retries 1 \
  --task-timeout 5m \
  --memory 512Mi \
  --quiet

echo "==> Allowing the scheduler to invoke the job"
gcloud run jobs add-iam-policy-binding "$JOB" \
  --region "$REGION" --project "$PROJECT" \
  --member "serviceAccount:${SA}" \
  --role roles/run.invoker >/dev/null

echo "==> Creating or updating the 15-minute schedule"
# --max-retries 1 on the job plus a 60s attempt deadline here: if a run is going
# to fail, fail it fast and let the next 15-minute tick recover via the 24-hour
# overlap window. Piling up retries just delays the run that would have worked.
if gcloud scheduler jobs describe "$SCHEDULER_JOB" \
      --project "$PROJECT" --location "$REGION" >/dev/null 2>&1; then
  ACTION=update
else
  ACTION=create
fi

gcloud scheduler jobs "$ACTION" http "$SCHEDULER_JOB" \
  --project "$PROJECT" \
  --location "$REGION" \
  --schedule "7,22,37,52 * * * *" \
  --time-zone UTC \
  --uri "https://run.googleapis.com/v2/projects/${PROJECT}/locations/${REGION}/jobs/${JOB}:run" \
  --http-method POST \
  --oauth-service-account-email "$SA" \
  --attempt-deadline 60s \
  --description "Trigger USGS ingest Cloud Run job every 15 minutes"

echo
echo "Deployed. Trigger one now with:"
echo "  gcloud scheduler jobs run ${SCHEDULER_JOB} --project ${PROJECT} --location ${REGION}"
echo "Tail logs with:"
echo "  gcloud beta run jobs logs tail ${JOB} --project ${PROJECT} --region ${REGION}"
