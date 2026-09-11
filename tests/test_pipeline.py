"""Offline tests. No network, no BigQuery -- run them anywhere.

    python -m pytest tests/ -q
"""

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import bq, usgs  # noqa: E402


def _feature(event_id, time_ms, updated_ms, mag=3.0):
    return {
        "id": event_id,
        "type": "Feature",
        "properties": {
            "time": time_ms,
            "updated": updated_ms,
            "mag": mag,
            "magType": "ml",
            "place": "somewhere",
            "status": "automatic",
            "type": "earthquake",
            "net": "nc",
            "tsunami": 0,
            "sig": 100,
        },
        "geometry": {"coordinates": [-122.0, 37.0, 5.5]},
    }


def test_column_order_matches_schema():
    """The guard that stops MERGE's positional INSERT ROW writing into the wrong
    column. If this fails, do not ship -- latitude would land in longitude."""
    assert bq.column_order_matches_schema()


def test_schema_sql_lists_same_columns():
    """sql/schema.sql is the third definition of the column list. Keep it honest."""
    ddl = (Path(__file__).resolve().parent.parent / "sql" / "schema.sql").read_text()
    events_ddl = ddl.split("CREATE TABLE IF NOT EXISTS `{dataset}.events_staging`")[0]
    for column in usgs.COLUMNS:
        assert f"  {column} " in events_ddl or f"  {column:<11}" in events_ddl, column


def test_normalize_maps_coordinates_correctly():
    """GeoJSON is [longitude, latitude, depth] -- the opposite of how people say
    it. Getting this backwards puts every earthquake in the wrong hemisphere and
    nothing crashes."""
    rows = usgs.normalize({"features": [_feature("a1", 1_700_000_000_000, 1_700_000_001_000)]}, "t")
    assert len(rows) == 1
    assert rows[0]["longitude"] == -122.0
    assert rows[0]["latitude"] == 37.0
    assert rows[0]["depth_km"] == 5.5


def test_dedupe_keeps_newest_revision():
    """A batch containing one id twice would make BigQuery's MERGE throw
    ('UPDATE/MERGE must match at most one source row'). Keep the newest."""
    rows = usgs.normalize(
        {
            "features": [
                _feature("dup", 1_700_000_000_000, 1_700_000_001_000, mag=3.0),
                _feature("dup", 1_700_000_000_000, 1_700_000_999_000, mag=4.4),
            ]
        },
        "t",
    )
    assert len(rows) == 1
    assert rows[0]["mag"] == 4.4


def test_normalize_skips_features_without_id():
    rows = usgs.normalize({"features": [{"properties": {"time": 1}, "geometry": {}}]}, "t")
    assert rows == []


def test_normalize_survives_missing_fields():
    """USGS nulls plenty of fields on fresh automatic events. None of it should
    raise -- a run that crashes on a null magnitude is a run that skips an hour."""
    rows = usgs.normalize(
        {"features": [{"id": "x", "properties": {"time": 1_700_000_000_000}, "geometry": {}}]},
        "t",
    )
    assert len(rows) == 1
    assert rows[0]["mag"] is None
    assert rows[0]["latitude"] is None
    assert rows[0]["event_time"] is not None


def test_split_statements_handles_semicolon_inside_string():
    """The bug this file exists to prevent regressing: a semicolon inside a
    column description used to slice the DDL in half."""
    sql = 'CREATE TABLE a (x STRING OPTIONS(description="one; two"));\nCREATE TABLE b (y INT64);'
    statements = list(bq._split_statements(sql))
    assert len(statements) == 2
    assert 'description="one; two"' in statements[0]


def test_split_statements_ignores_comments():
    sql = "-- leading comment; with semicolon\nCREATE TABLE a (x INT64);"
    statements = list(bq._split_statements(sql))
    assert len(statements) == 1
    assert statements[0].startswith("CREATE TABLE")


def test_ms_to_dt_is_utc():
    value = usgs._ms_to_dt(1_700_000_000_000)
    assert value.tzinfo is not None
    assert value.utcoffset() == dt.timedelta(0)


def test_ms_to_dt_tolerates_garbage():
    assert usgs._ms_to_dt("not-a-number") is None
    assert usgs._ms_to_dt(None) is None
