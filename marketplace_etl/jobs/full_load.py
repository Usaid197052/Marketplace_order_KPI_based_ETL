from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from marketplace_etl import common
from marketplace_etl.settings import load_settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Marketplace orders full load")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    common.setup_logging()
    log = common.LOGGER
    settings = load_settings()

    ch = common.clickhouse_client(settings)
    common.bootstrap_schema(ch, settings)

    conn = common.open_source_snapshot(settings)
    total_rows = 0
    try:
        controls = common.capture_source_controls(conn, settings)
        log.info(
            "Source snapshot: rows=%s sum_grand_total=%s max_updated_at=%s",
            controls.row_count,
            controls.sum_grand_total,
            controls.max_updated_at,
        )
        spark = common.build_spark("marketplace_orders_full_load")
        extracted_at = common.utcnow_naive()
        try:
            for page in common.iter_full_pages(conn, settings):
                tuples = common.rows_to_raw_tuples(spark, page, extracted_at)
                common.insert_raw_rows(ch, settings, tuples)
                total_rows += len(tuples)
                log.info("Loaded %s rows (last id %s)", total_rows, page[-1][0])
        finally:
            spark.stop()
    finally:
        common.close_source(conn)

    common.store_run_controls(ch, settings, args.run_id, controls, total_rows)
    log.info(
        "Full load complete: extracted=%s source_count=%s (acceptance totals are "
        "re-checked by the publish step)",
        total_rows,
        controls.row_count,
    )


if __name__ == "__main__":
    main()
