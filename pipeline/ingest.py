"""The recurring ingest job. Runs every 15 minutes on GitHub Actions.

One run does: figure out whether the live feed is enough, fetch, stage, MERGE,
and write an audit row -- whether or not anything new showed up.

    python -m pipeline.ingest
    python -m pipeline.ingest --dry-run     # fetch and report, write nothing
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import traceback
import uuid

from pipeline import bq, config, usgs

log = logging.getLogger("ingest")


def now():
    return dt.datetime.now(dt.timezone.utc)


def run_identity():
    """Where this run came from, so a post-mortem can find the logs.

    Two schedulers drive this same code -- GitHub Actions and Cloud Run Jobs --
    and both write to the same table. Recording which one produced a row is what
    makes the continuity ledger readable when they are both running, and it is
    how you tell "GitHub stopped firing" apart from "the pipeline is down".
    """
    gh_run_id = os.environ.get("GITHUB_RUN_ID")
    if gh_run_id:
        server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        return {
            "run_id": f"gha-{gh_run_id}",
            "trigger": f"github-{os.environ.get('GITHUB_EVENT_NAME', 'schedule')}",
            "runner_url": f"{server}/{repo}/actions/runs/{gh_run_id}",
        }

    # Cloud Run Jobs inject these; CLOUD_RUN_EXECUTION is unique per execution.
    execution = os.environ.get("CLOUD_RUN_EXECUTION")
    if execution:
        project = os.environ.get("GCP_PROJECT", "")
        region = os.environ.get("CLOUD_RUN_REGION", "us-central1")
        job = os.environ.get("CLOUD_RUN_JOB", "")
        return {
            "run_id": f"run-{execution}",
            "trigger": "cloud-run-scheduler",
            "runner_url": (
                f"https://console.cloud.google.com/run/jobs/details/"
                f"{region}/{job}/executions?project={project}"
            ),
        }

    return {"run_id": f"local-{uuid.uuid4().hex[:12]}", "trigger": "local", "runner_url": None}


def decide_mode(bq_client, started_at):
    """Live feed, or escalate to a range query?

    The live feed covers a fixed 24-hour window. If our newest event is older
    than that, the feed physically cannot close the gap -- polling it again would
    leave a permanent hole while looking perfectly healthy. So we check first and
    escalate to an FDSN range query when we have fallen too far behind.

    This is also the seed/trickle bridge. The seed stops at midnight of the day it
    ran; the first scheduled run afterwards sees a >20h gap and backfills it. No
    manual step, no hole between history and live.
    """
    latest = bq.latest_event_time(bq_client)

    if latest is None:
        log.warning("events table is empty -- has the seed been loaded? Falling back to feed.")
        return "feed", None, None

    gap = started_at - latest
    gap_hours = gap.total_seconds() / 3600
    log.info("newest event held: %s (%.1f hours old)", latest, gap_hours)

    if gap_hours <= config.BACKFILL_THRESHOLD_HOURS:
        return "feed", None, None

    # Overlap by an hour so a revision right at the boundary is not missed.
    window_start = latest - dt.timedelta(hours=1)
    window_end = min(started_at, window_start + dt.timedelta(days=config.MAX_BACKFILL_DAYS))
    log.warning(
        "gap of %.1fh exceeds %.1fh threshold -- backfilling %s -> %s",
        gap_hours,
        config.BACKFILL_THRESHOLD_HOURS,
        window_start,
        window_end,
    )
    return "backfill", window_start, window_end


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Fetch and report; write nothing.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if not bq.column_order_matches_schema():
        raise SystemExit("column order drift between usgs.COLUMNS and bq.STAGING_SCHEMA")

    identity = run_identity()
    started_at = now()

    record = {
        "run_id": identity["run_id"],
        "started_at": started_at,
        "finished_at": None,
        "duration_ms": None,
        "trigger": identity["trigger"],
        "mode": None,
        "window_start": None,
        "window_end": None,
        "rows_fetched": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "status": "error",
        "error_message": None,
        "runner_url": identity["runner_url"],
    }

    bq_client = bq.client()
    failed = False

    try:
        mode, window_start, window_end = decide_mode(bq_client, started_at)
        record["mode"] = mode
        record["window_start"] = window_start
        record["window_end"] = window_end

        if mode == "backfill":
            rows = usgs.fetch_range(window_start, window_end, source="backfill")
        else:
            rows = usgs.fetch_feed(config.FEED_WINDOW)

        record["rows_fetched"] = len(rows)
        log.info("fetched %d events (mode=%s)", len(rows), mode)

        if args.dry_run:
            # Returning here still runs the `finally` block, which is where the
            # summary gets written -- so set the status rather than reporting it
            # twice on the way out.
            log.info("dry run -- not writing")
            record["status"] = "dry_run"
            return

        if rows:
            staging = bq.load_rows_to_staging(bq_client, rows, identity["run_id"])
            try:
                inserted, updated = bq.merge_staging_into_events(bq_client, staging)
            finally:
                bq.drop_staging(bq_client, staging)
            record["rows_inserted"] = inserted
            record["rows_updated"] = updated
            record["status"] = "ok" if (inserted or updated) else "no_new_data"
        else:
            # A genuinely quiet window. Normal, not a bug -- but it still gets an
            # audit row, because "quiet" and "dead" must not look the same later.
            record["status"] = "no_new_data"

    except Exception as exc:  # noqa: BLE001 -- we record the failure, then re-raise
        failed = True
        record["status"] = "error"
        record["error_message"] = f"{type(exc).__name__}: {exc}"[:1000]
        log.error("run failed: %s", record["error_message"])
        log.debug(traceback.format_exc())

    finally:
        finished_at = now()
        record["finished_at"] = finished_at
        record["duration_ms"] = int((finished_at - started_at).total_seconds() * 1000)

        if not args.dry_run:
            try:
                bq.record_run(bq_client, record)
            except Exception as exc:  # noqa: BLE001
                # Never let audit-logging failure hide the real error.
                log.error("could not write ingest_runs row: %s", exc)

        _summarize(record, dry_run=args.dry_run)

    if failed:
        # Exit non-zero on purpose. A silently-green failing pipeline is the
        # thing the follow-up assignment is built to catch. We would rather have
        # a red X in the Actions tab and an email. Recovery is automatic: the
        # next run's 24-hour overlap re-fetches whatever this run missed.
        sys.exit(1)


def _summarize(record, dry_run=False):
    """Write a human-readable summary to the Actions run page."""
    lines = [
        "### USGS ingest run",
        "",
        f"- **status**: `{record['status']}`" + (" _(dry run)_" if dry_run else ""),
        f"- **mode**: `{record['mode']}`",
        f"- **fetched**: {record['rows_fetched']}",
        f"- **inserted**: {record['rows_inserted']}",
        f"- **updated**: {record['rows_updated']}",
        f"- **duration**: {record['duration_ms']} ms",
    ]
    if record["window_start"]:
        lines.append(f"- **backfill window**: {record['window_start']} -> {record['window_end']}")
    if record["error_message"]:
        lines.append(f"- **error**: `{record['error_message']}`")

    summary = "\n".join(lines)
    print(summary)

    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(summary + "\n")


if __name__ == "__main__":
    main()
