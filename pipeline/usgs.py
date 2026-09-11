"""Shared USGS access and normalization.

Both the one-time historical seed and the recurring ingest job go through this
module, so a GeoJSON feature becomes the same row shape no matter which path it
arrived on. That is what makes the seed and the trickle MERGE-compatible.
"""

from __future__ import annotations

import datetime as dt
import logging
import time
from typing import Any

import requests

log = logging.getLogger(__name__)

FDSN_QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query"
FDSN_COUNT = "https://earthquake.usgs.gov/fdsnws/event/1/count"
FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{window}.geojson"

# FDSN caps a single response at 20k events. Staying under it means a chunk
# never silently truncates.
MAX_EVENTS_PER_REQUEST = 18_000

USER_AGENT = "byu-cs452-pipeline-sample/1.0 (course reference implementation)"

# Columns written to BigQuery, in order. Keep in sync with sql/schema.sql.
COLUMNS = [
    "id",
    "event_time",
    "updated",
    "mag",
    "mag_type",
    "place",
    "latitude",
    "longitude",
    "depth_km",
    "status",
    "event_type",
    "net",
    "tsunami",
    "sig",
    "felt",
    "cdi",
    "mmi",
    "alert",
    "nst",
    "dmin",
    "rms",
    "gap",
    "url",
    "source",
    "ingested_at",
]


class UsgsUnavailable(RuntimeError):
    """USGS returned something we should retry later rather than crash on."""


def _get(url, params=None, attempts=5):
    """GET with exponential backoff.

    USGS 503s under load. An unbounded /count over the whole catalog reliably
    returns "General error: 1114 ... table is full" -- retrying helps a transient
    overload, but not a query that is simply too big. That is why the seed
    bisects time ranges instead of asking for everything at once.
    """
    last = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                timeout=180,
                headers={"User-Agent": USER_AGENT},
            )
        except requests.RequestException as exc:
            last = exc
            log.warning("request error (attempt %d/%d): %s", attempt, attempts, exc)
        else:
            # 204 means a valid query with zero matches -- not an error.
            if resp.status_code in (200, 204):
                return resp
            last = UsgsUnavailable(f"HTTP {resp.status_code}: {resp.text[:300]}")
            log.warning("bad status (attempt %d/%d): %s", attempt, attempts, last)

        if attempt < attempts:
            time.sleep(min(2**attempt, 30))

    raise UsgsUnavailable(f"giving up on {url} after {attempts} attempts") from last


def count_events(start, end):
    """How many events fall in [start, end)? Used to size seed chunks."""
    resp = _get(FDSN_COUNT, {"starttime": _iso(start), "endtime": _iso(end), "format": "text"})
    if resp.status_code == 204 or not resp.text.strip():
        return 0
    return int(resp.text.strip())


def fetch_range(start, end, source="seed"):
    """All events in [start, end) as normalized rows. Caller keeps chunks small."""
    resp = _get(
        FDSN_QUERY,
        {
            "starttime": _iso(start),
            "endtime": _iso(end),
            "format": "geojson",
            "orderby": "time-asc",
        },
    )
    if resp.status_code == 204:
        return []
    return normalize(resp.json(), source=source)


def fetch_feed(window="all_day"):
    """Current live feed. Windows: all_hour, all_day, all_week, all_month.

    We poll all_day rather than all_hour on purpose -- see README, "Why a
    24-hour overlap window". It makes every run self-healing for outages up to
    a day.
    """
    resp = _get(FEED.format(window=window))
    if resp.status_code == 204:
        return []
    return normalize(resp.json(), source="feed")


def normalize(geojson, source):
    """GeoJSON FeatureCollection -> flat rows matching COLUMNS."""
    now = dt.datetime.now(dt.timezone.utc)
    rows = []

    for feature in geojson.get("features", []):
        props = feature.get("properties") or {}
        coords = (feature.get("geometry") or {}).get("coordinates") or []
        event_id = feature.get("id")
        if not event_id:
            continue  # no primary key, no row

        rows.append(
            {
                "id": event_id,
                "event_time": _ms_to_dt(props.get("time")),
                "updated": _ms_to_dt(props.get("updated")),
                "mag": _float(props.get("mag")),
                "mag_type": props.get("magType"),
                "place": props.get("place"),
                "longitude": _float(coords[0]) if len(coords) > 0 else None,
                "latitude": _float(coords[1]) if len(coords) > 1 else None,
                "depth_km": _float(coords[2]) if len(coords) > 2 else None,
                "status": props.get("status"),
                "event_type": props.get("type"),
                "net": props.get("net"),
                "tsunami": bool(props.get("tsunami")),
                "sig": _int(props.get("sig")),
                "felt": _int(props.get("felt")),
                "cdi": _float(props.get("cdi")),
                "mmi": _float(props.get("mmi")),
                "alert": props.get("alert"),
                "nst": _int(props.get("nst")),
                "dmin": _float(props.get("dmin")),
                "rms": _float(props.get("rms")),
                "gap": _float(props.get("gap")),
                "url": props.get("url"),
                "source": source,
                "ingested_at": now,
            }
        )

    return dedupe(rows)


def dedupe(rows):
    """Keep the newest revision per id *within this batch*.

    BigQuery MERGE refuses to update the same target row twice -- a batch
    containing an id twice is a runtime error, not a silent overwrite. The live
    feed genuinely can repeat an id when an event is revised mid-fetch, and
    overlapping seed chunks can repeat one at a boundary.
    """
    best = {}
    for row in rows:
        prior = best.get(row["id"])
        if prior is None or _sortable(row["updated"]) >= _sortable(prior["updated"]):
            best[row["id"]] = row
    return list(best.values())


def _sortable(value):
    return value or dt.datetime.min.replace(tzinfo=dt.timezone.utc)


def _iso(value):
    return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _ms_to_dt(value):
    if value is None:
        return None
    try:
        return dt.datetime.fromtimestamp(int(value) / 1000, tz=dt.timezone.utc)
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def _float(value):
    try:
        return None if value is None else float(value)
    except (ValueError, TypeError):
        return None


def _int(value):
    try:
        return None if value is None else int(value)
    except (ValueError, TypeError):
        return None
