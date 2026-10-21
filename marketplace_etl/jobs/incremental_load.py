from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from marketplace_etl import common, watermarking
from marketplace_etl.settings import load_settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Marketplace orders incremental load")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    common.setup_logging()
    log = common.LOGGER
    settings = load_settings()

    ch = common.clickhouse_client(settings)
    common.bootstrap_schema(ch, settings)

    watermark = watermarking.require_watermark(common.get_watermark(ch, settings))
    lower_bound = watermarking.extraction_lower_bound(watermark)
    log.info("Watermark %s -> extraction window from %s", watermark, lower_bound)

    conn = common.open_source_snapshot(settings)
    try:
        controls = common.capture_source_controls(conn, settings)
        rows = common.fetch_incremental_rows(conn, settings, lower_bound)
    finally:
        common.close_source(conn)

    log.info("Window returned %s rows", len(rows))
    if rows:
        spark = common.build_spark("marketplace_orders_incremental")
        extracted_at = common.utcnow_naive()
        try:
            for chunk in common.chunked(rows, settings.insert_batch_size):
                tuples = common.rows_to_raw_tuples(spark, chunk, extracted_at)
                common.insert_raw_rows(ch, settings, tuples)
        finally:
            spark.stop()

    common.store_run_controls(ch, settings, args.run_id, controls, len(rows))
    log.info("Incremental extract complete: %s rows upserted into raw", len(rows))


if __name__ == "__main__":
    main()
