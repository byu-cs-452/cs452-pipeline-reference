# The one-time bulk load

Two scripts, run in order, exactly once:

```bash
python -m seed.download_history --start-year 1970   # network -> data/raw/*.parquet
python -m seed.load_seed                            # data/raw/ -> BigQuery
```

## Seeding etiquette

`download_history.py` is the only thing here that touches the network, and it is
designed to run **once**:

- Output lands in `data/raw/`, which is gitignored — large, regenerable, and not
  something to push through git.
- `--resume` skips any chunk already on disk, so an interrupted run picks up where it
  stopped instead of starting over.
- `load_seed.py` reads **only from disk**. Iterate on the load, the schema, or the MERGE
  as many times as you like without sending a single additional request to USGS.

USGS is a free public service with no API key and no rate limit to hide behind. A class
re-downloading fifty years of history on every iteration is how that stops being true.
Reading local parquet is also about 100× faster, so the etiquette and the ergonomics
point the same direction.

## How it decides what to request

FDSN caps a response at 20,000 events, so history has to be cut into chunks that each
fit. A fixed chunk size doesn't work — 1975 has ~9,000 events and 2024 has ~156,000.

So the seeder **bisects on `/count`**: ask how many events are in a range, and if the
answer exceeds 18,000, split it in half and ask again about each side. Sparse decades
cost one request; dense months get split until they fit.

This also avoids a real failure. USGS answers an unbounded count over the full catalog
with:

```
503 ... SQLSTATE[HY000]: General error: 1114 The table '/rdsdbdata/tmp/#sql...' is full
```

Their backend cannot scan the whole catalog for one query. Planning proceeds a year at a
time so no request is ever unbounded — if you extend this script, preserve that.

## Why this isn't the hourly job

Different problem, different tool. The bulk load moves millions of rows, runs on a
laptop with local disk, and takes half an hour. The ingest job moves a few hundred rows,
runs on a hosted runner, and finishes in seconds. A GitHub runner has a 6-hour ceiling
and no disk worth using; conversely, standing up serverless infrastructure to do
something once is pure ceremony. Both paths share `pipeline/usgs.py`, so they normalize
identically and the results MERGE cleanly — but the compute around them is sized for the
job it actually has.
