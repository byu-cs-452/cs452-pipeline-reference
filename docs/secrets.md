# Secrets management

**Summary: this repo contains no secret, and no long-lived credential exists anywhere.**

## What we did: Workload Identity Federation

GitHub Actions authenticates to Google Cloud with a short-lived OIDC token instead of a
stored key:

1. The runner asks GitHub for an OIDC token asserting *"I am a job running in
   `byu-cs-452/cs452-pipeline-reference`."* This requires `permissions: id-token: write`.
2. Google's STS validates that token against a **workload identity pool provider**
   configured to trust GitHub's issuer, with an attribute condition
   (`assertion.repository_owner == 'byu-cs-452'`).
3. STS exchanges it for a ~1-hour access token that can impersonate the service account —
   but only for principals matching
   `attribute.repository/byu-cs-452/cs452-pipeline-reference`.

Nothing to rotate, nothing to leak, nothing to accidentally commit. A fork, another repo
in the org, or a stolen copy of this code cannot mint a token, because the identity is
bound to the repository rather than to a string anybody can copy.

### Setup (once)

```bash
PROJECT=cs393-496021
PROJECT_NUMBER=620970916253
REPO=byu-cs-452/cs452-pipeline-reference
SA=usgs-pipeline-ingest@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts create usgs-pipeline-ingest --project=$PROJECT

# Least privilege: jobUser project-wide (running a query needs it), but write access
# scoped to the one dataset -- not project-wide bigquery.dataEditor.
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
# Dataset-level WRITER is granted via the BigQuery API; see pipeline/bootstrap notes.

gcloud iam workload-identity-pools create github-pool \
  --project=$PROJECT --location=global

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --project=$PROJECT --location=global --workload-identity-pool=github-pool \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository_owner == 'byu-cs-452'"

gcloud iam service-accounts add-iam-policy-binding $SA --project=$PROJECT \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/attribute.repository/$REPO"
```

> The attribute condition is not optional. Without one, *any* GitHub repository on the
> internet can present a valid token from that issuer. The condition plus the
> repo-scoped principalSet are the two things doing the actual security work.

### Repository variables (not secrets)

| Variable | Value |
|---|---|
| `GCP_PROJECT` | `cs393-496021` |
| `BQ_DATASET` | `usgs_pipeline` |
| `BQ_LOCATION` | `US` |
| `WIF_PROVIDER` | `projects/620970916253/.../providers/github-provider` |
| `WIF_SERVICE_ACCOUNT` | `usgs-pipeline-ingest@cs393-496021.iam.gserviceaccount.com` |

These are **variables**, not secrets, deliberately. None of them grants access — the
provider path and service account email are just names, and the security boundary is the
IAM binding. Storing non-secrets as secrets trains the wrong reflex: it makes "is this
actually sensitive?" a question nobody asks, and real secrets get lost in the noise.

## The simpler alternative: a service account key

Most solutions do this, and it earns full credit:

```bash
gcloud iam service-accounts keys create key.json --iam-account=$SA
gh secret set GCP_SA_KEY < key.json
rm key.json          # do not skip this line
```

```yaml
- uses: google-github-actions/auth@v2
  with:
    credentials_json: ${{ secrets.GCP_SA_KEY }}
```

It works, and it's one step instead of four. The tradeoff is that you've created a
permanent credential: a JSON file that grants BigQuery write access to whoever holds it,
with no expiry, sitting in a settings page and in your shell history and possibly in
your Downloads folder. WIF's token expires in an hour and cannot be copied out of the
runner to any useful end.

The assignment's warning — a committed credential costs points "because it will one day
not be [private]" — is about exactly this. If you use a key, at minimum: never write it
into the repo, delete the local copy immediately, and rotate it when the project ends.

## What is *not* a secret here

USGS needs no API key. If you picked EIA, your key would be the one real secret in the
project: `gh secret set EIA_API_KEY`, read via `os.environ`, never a default value in
code. A default API key in source is still a committed credential.
