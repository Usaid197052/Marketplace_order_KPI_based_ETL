from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from marketplace_etl import common, schema, watermarking
from marketplace_etl.settings import load_settings


_FLAG_SELECT_METADATA = ["_extracted_at", "toUInt8(1)", "now()", "now64(3)"]


def flag_deleted_ids(client, settings, missing_ids: list[int]) -> None:
    raw = common.qualified(settings, common.RAW_TABLE)
    column_list = ", ".join(schema.ALL_RAW_COLUMN_NAMES)
    select_list = ", ".join(schema.SOURCE_COLUMN_NAMES + _FLAG_SELECT_METADATA)
    sql = (
        f"INSERT INTO {raw} ({column_list}) "
        f"SELECT {select_list} FROM {raw} FINAL "
        "WHERE _is_deleted = 0 AND id IN {ids:Array(UInt64)}"
    )
    for chunk in common.chunked(missing_ids, 10_000):
        client.command(sql, parameters={"ids": list(chunk)})


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Marketplace orders delete reconcile")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    common.setup_logging()
    log = common.LOGGER
    settings = load_settings()

    ch = common.clickhouse_client(settings)
    common.bootstrap_schema(ch, settings)

    watermark = watermarking.require_watermark(common.get_watermark(ch, settings))
    lower_bound = watermarking.extraction_lower_bound(watermark)

    conn = common.open_source_snapshot(settings)
    try:
        controls = common.capture_source_controls(conn, settings)
        rows = common.fetch_incremental_rows(conn, settings, lower_bound)
        source_ids: set[int] = set()
        for page in common.iter_source_ids(conn, settings):
            source_ids.update(page)
    finally:
        common.close_source(conn)

    log.info(
        "Snapshot: %s source ids, %s rows in the refresh window", len(source_ids), len(rows)
    )

    if rows:
        spark = common.build_spark("marketplace_orders_reconcile")
        extracted_at = common.utcnow_naive()
        try:
            for chunk in common.chunked(rows, settings.insert_batch_size):
                tuples = common.rows_to_raw_tuples(spark, chunk, extracted_at)
                common.insert_raw_rows(ch, settings, tuples)
        finally:
            spark.stop()

    raw = common.qualified(settings, common.RAW_TABLE)
    warehouse_ids = {
        r[0]
        for r in ch.query(f"SELECT id FROM {raw} FINAL WHERE _is_deleted = 0").result_rows
    }
    missing = sorted(warehouse_ids - source_ids)
    if missing:
        flag_deleted_ids(ch, settings, missing)
        log.info("Flagged %s hard-deleted ids: %s...", len(missing), missing[:20])
    else:
        log.info("No hard deletes found")

    common.store_run_controls(ch, settings, args.run_id, controls, len(rows))


if __name__ == "__main__":
    main()
