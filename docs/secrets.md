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
PROJECT=cs452-508317
PROJECT_NUMBER=168005390822
REPO=byu-cs-452/cs452-pipeline-reference
SA=usgs-pipeline-ingest@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts create usgs-pipeline-ingest --project=$PROJECT

# Least privilege: jobUser project-wide (running a query needs it), but write access
# scoped to the one dataset -- not project-wide bigquery.dataEditor.
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
# Dataset-level WRITER, after `python -m pipeline.bootstrap` has created the dataset.
# bq has no one-liner for dataset ACLs: dump the dataset, append
#   {"role": "WRITER", "userByEmail": "<the SA email>"}
# to its "access" array, and push it back.
bq show --format=prettyjson $PROJECT:usgs_pipeline > dataset.json
bq update --source dataset.json $PROJECT:usgs_pipeline

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

### This repo is public. Why that is still safe

Worth spelling out, because "public repo + cloud credentials" is where this setup most
often goes wrong. Three independent things have to hold, and all three were verified:

1. **The provider rejects other owners.** `assertion.repository_owner == 'byu-cs-452'`.
   Fork this repo to your own account, and the token your fork presents fails the
   condition at Google's STS before any service account is involved.
2. **The binding names one repository.** Impersonation is granted only to
   `principalSet://.../attribute.repository/byu-cs-452/cs452-pipeline-reference`. Even
   another repo *inside* this org cannot assume the identity.
3. **Forks cannot trigger the workflow at all.** It fires on `schedule` and
   `workflow_dispatch` only. There is no `pull_request` trigger, so a malicious PR has
   no path to execution — and independently, GitHub withholds `id-token: write` from
   fork-PR workflows precisely to stop this attack.

Verify 1 and 2 yourself:

```bash
gcloud iam workload-identity-pools providers describe github-provider \
  --project=cs452-508317 --location=global --workload-identity-pool=github-pool \
  --format="value(attributeCondition)"

gcloud iam service-accounts get-iam-policy \
  usgs-pipeline-ingest@cs452-508317.iam.gserviceaccount.com \
  --project=cs452-508317 --format="value(bindings.members)"
```

**Contrast this with the service-account-key approach.** Had we put a JSON key in
`secrets.GCP_SA_KEY`, going public would be survivable but far more fragile: secrets are
withheld from fork PRs, but one workflow change adding a `pull_request` trigger, or one
`echo` of the wrong variable, exfiltrates a permanent credential. With WIF the worst case
is a one-hour token scoped to one dataset — and an attacker cannot obtain even that
without commit access to this specific repository. **Going public is a decision you make
about the code; with a stored key it silently becomes a decision about the credential
too.**

### Repository variables (not secrets)

| Variable | Value |
|---|---|
| `GCP_PROJECT` | `cs452-508317` |
| `BQ_DATASET` | `usgs_pipeline` |
| `BQ_LOCATION` | `US` |
| `WIF_PROVIDER` | `projects/168005390822/.../providers/github-provider` |
| `WIF_SERVICE_ACCOUNT` | `usgs-pipeline-ingest@cs452-508317.iam.gserviceaccount.com` |

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
