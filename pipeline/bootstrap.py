"""Create the dataset and tables. Idempotent -- run it as often as you like.

    python -m pipeline.bootstrap
"""

import logging

from pipeline import bq, config


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    if not bq.column_order_matches_schema():
        raise SystemExit("column order drift between usgs.COLUMNS and bq.STAGING_SCHEMA")

    client = bq.client()
    bq.ensure_dataset(client)
    bq.apply_schema(client)

    tables = sorted(t.table_id for t in client.list_tables(config.dataset_ref()))
    print(f"\n{config.dataset_ref()} contains: {', '.join(tables)}")


if __name__ == "__main__":
    main()
