"""Configuration, all of it from the environment.

Nothing secret lives in this repo. Locally these come from a .env you never
commit (or just your gcloud ADC); in GitHub Actions the project id comes from a
repository variable and the *credential* comes from Workload Identity Federation,
which means there is no key to commit in the first place. See docs/secrets.md.
"""

import os

GCP_PROJECT = os.environ.get("GCP_PROJECT", "cs452-508317")
BQ_DATASET = os.environ.get("BQ_DATASET", "usgs_pipeline")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# Live feed window polled on every run. all_day gives a 24-hour overlap, which
# is what makes a missed run self-healing. See README, "Why a 24-hour window".
FEED_WINDOW = os.environ.get("FEED_WINDOW", "all_day")

# If the newest event we hold is older than this, the feed alone cannot close
# the gap and the run escalates to an FDSN range query instead.
BACKFILL_THRESHOLD_HOURS = float(os.environ.get("BACKFILL_THRESHOLD_HOURS", "20"))

# Refuse to run a backfill wider than this in one go; a longer outage gets
# repaired over several runs rather than one enormous request to a free service.
MAX_BACKFILL_DAYS = float(os.environ.get("MAX_BACKFILL_DAYS", "30"))


def dataset_ref():
    return f"{GCP_PROJECT}.{BQ_DATASET}"


def table(name):
    return f"{GCP_PROJECT}.{BQ_DATASET}.{name}"
