"""Fail loudly if the pipeline has gone quiet.

The ingest job already fails loudly when it *errors*. This catches the nastier
case: a pipeline that reports green while writing nothing. Runs green, exits 0,
writes no rows, and nobody notices for a week.

    python -m pipeline.healthcheck
    python -m pipeline.healthcheck --max-age-minutes 90

Exits non-zero when data is stale, which turns into a red X and an email.

A caveat worth stating plainly, because it is the most common way monitoring
lies: this check runs on the same GitHub Actions schedule as the thing it
watches. If Actions stops running workflows for this repo -- the 60-day
inactivity disable, an outage, a billing problem -- then the ingest job and its
watchdog die together and *nothing alerts*. A monitor that shares fate with the
monitored system is not really a monitor. Genuinely independent monitoring means
a different provider (an uptime service hitting a freshness endpoint, a Cloud
Scheduler job in GCP). That is out of scope here, but the gap is real and you
should know it exists rather than trust a green check that cannot go red.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

from pipeline import bq, config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-age-minutes",
        type=int,
        default=90,
        help="Fail if nothing has been written in this long. Default 90 = six "
        "missed 15-minute runs, loose enough to absorb GitHub queue delay.",
    )
    args = parser.parse_args()

    client = bq.client()
    events = config.table("events")
    runs = config.table("ingest_runs")

    freshness = list(
        client.query(
            f"""
            SELECT
              MAX(ingested_at) AS last_write,
              TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), MINUTE) AS age_minutes,
              COUNT(*) AS total_events
            FROM `{events}`
            """
        ).result()
    )[0]

    recent = list(
        client.query(
            f"""
            SELECT
              COUNTIF(status = 'ok')           AS ok_runs,
              COUNTIF(status = 'no_new_data')  AS quiet_runs,
              COUNTIF(status = 'error')        AS failed_runs,
              COUNT(*)                         AS total_runs
            FROM `{runs}`
            WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """
        ).result()
    )[0]

    age = freshness.age_minutes
    stale = age is None or age > args.max_age_minutes

    # 96 runs a day expected; warn well before it looks catastrophic.
    runs_low = (recent.total_runs or 0) < 60

    lines = [
        "### Pipeline health",
        "",
        f"- **last write**: {freshness.last_write} ({age} minutes ago)",
        f"- **total events**: {freshness.total_events:,}",
        f"- **runs in last 24h**: {recent.total_runs} (expected ~96)",
        f"  - with new data: {recent.ok_runs}",
        f"  - quiet: {recent.quiet_runs}",
        f"  - failed: {recent.failed_runs}",
        "",
        f"**verdict**: {'STALE' if stale else 'healthy'}"
        + (" / RUNS BELOW EXPECTED" if runs_low else ""),
    ]

    if stale:
        lines += [
            "",
            f"Nothing written in {age} minutes (threshold {args.max_age_minutes}).",
            "Start at docs/runbook.md -> 'Is it alive?'.",
        ]

    report = "\n".join(lines)
    print(report)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write(report + "\n")

    if stale:
        sys.exit(1)
    if runs_low:
        # Not stale but under-running: worth a look, not worth a page.
        print(f"::warning::only {recent.total_runs} runs in 24h, expected ~96")


if __name__ == "__main__":
    main()
