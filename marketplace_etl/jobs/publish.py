from __future__ import annotations

import argparse
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from marketplace_etl import common, schema, transforms, watermarking
from marketplace_etl.settings import load_settings

SUM_TOLERANCE = Decimal("1")


def build_tables(ch, settings) -> None:
    raw = common.qualified(settings, common.RAW_TABLE)
    stg_build = common.qualified(settings, common.STG_BUILD_TABLE)
    fact_build = common.qualified(settings, common.FACT_BUILD_TABLE)
    columns = ", ".join(schema.STAGING_COLUMN_NAMES)

    ch.command(f"TRUNCATE TABLE {stg_build}")
    ch.command(
        f"INSERT INTO {stg_build} ({columns})\nSELECT\n    "
        + transforms.staging_select_sql()
        + f"\nFROM {raw} FINAL\nWHERE _is_deleted = 0"
    )
    ch.command(f"TRUNCATE TABLE {fact_build}")
    ch.command(f"INSERT INTO {fact_build} ({columns}) SELECT {columns} FROM {stg_build}")


def run_publish_tests(ch, settings, controls, mode: str) -> list[str]:
    fact_build = common.qualified(settings, common.FACT_BUILD_TABLE)
    failures: list[str] = []

    count, sum_gt, sum_comm, sum_seller, max_updated, tie_out_rows = ch.query(
        "SELECT count(), sum(grand_total), sum(commission), sum(seller_total), "
        f"max(updated_at), countIf(abs(gmv_tie_out_gap) > 1) FROM {fact_build}"
    ).result_rows[0]
    max_updated = common.as_naive(max_updated)


    if int(count) != controls.row_count:
        message = (
            f"test 1 row count: warehouse={count} source={controls.row_count} "
            f"(delta {int(count) - controls.row_count})"
        )
        if mode == "incremental" and int(count) > controls.row_count:
            message += (
                " - warehouse has rows the source no longer has; trigger the "
                "marketplace_orders_reconcile_deletes DAG to flag hard deletes"
            )
        failures.append(message)


    for name, warehouse_value, source_value in (
        ("grand_total", sum_gt, controls.sum_grand_total),
        ("commission", sum_comm, controls.sum_commission),
        ("seller_total", sum_seller, controls.sum_seller_total),
    ):
        if abs(Decimal(warehouse_value) - source_value) > SUM_TOLERANCE:
            failures.append(
                f"test 2 sum({name}): warehouse={warehouse_value} source={source_value}"
            )


    duplicates = ch.query(
        "SELECT count() FROM (SELECT marketplace_seller_id, order_id "
        f"FROM {fact_build} GROUP BY marketplace_seller_id, order_id "
        "HAVING count() > 1)"
    ).result_rows[0][0]
    if int(duplicates) != 0:
        failures.append(f"test 3 duplicate (marketplace_seller_id, order_id): {duplicates}")


    if int(tie_out_rows) > settings.tie_out_baseline_rows:
        failures.append(
            f"test 4 tie-out rows: {tie_out_rows} > baseline {settings.tie_out_baseline_rows}"
        )


    view_def_rows = ch.query(
        "SELECT create_table_query FROM system.tables "
        "WHERE database = {db:String} AND name = 'vw_certified_revenue'",
        parameters={"db": settings.clickhouse.database},
    ).result_rows
    view_def = view_def_rows[0][0] if view_def_rows else ""
    if "business_state = 'completed'" not in view_def:
        failures.append(
            "test 5 certified view: vw_certified_revenue does not filter "
            "business_state = 'completed'"
        )


    if max_updated is None:
        failures.append("test 6 freshness: warehouse max(updated_at) is NULL")
    elif controls.max_updated_at is not None:
        lag = (controls.max_updated_at - max_updated).total_seconds()
        if lag > settings.freshness_limit_seconds:
            failures.append(
                f"test 6 freshness: warehouse lags source by {lag:.0f}s "
                f"(limit {settings.freshness_limit_seconds}s)"
            )
    return failures


def exchange_and_reload(ch, settings) -> None:
    stg = common.qualified(settings, common.STG_TABLE)
    stg_build = common.qualified(settings, common.STG_BUILD_TABLE)
    fact = common.qualified(settings, common.FACT_TABLE)
    fact_build = common.qualified(settings, common.FACT_BUILD_TABLE)
    kpi = common.qualified(settings, common.KPI_TABLE)

    ch.command(f"EXCHANGE TABLES {stg_build} AND {stg}")
    ch.command(f"EXCHANGE TABLES {fact_build} AND {fact}")

    kpi_columns = (
        "date_role, month, type, rows_count, order_count, gmv, commission, "
        "seller_payable, net_merchandise, discount_amount, refunded"
    )
    ch.command(f"TRUNCATE TABLE {kpi}")
    ch.command(
        f"INSERT INTO {kpi} ({kpi_columns}) "
        "SELECT 'cohort' AS date_role, toStartOfMonth(assumeNotNull(created_date)) AS month, "
        "type, count(), uniqExact(order_id), sum(grand_total), sum(commission), "
        "sum(seller_total), sum(net_merchandise), sum(discount_amount), "
        f"sum(grand_total_refunded) FROM {fact} FINAL "
        "WHERE created_date IS NOT NULL GROUP BY month, type"
    )
    ch.command(
        f"INSERT INTO {kpi} ({kpi_columns}) "
        "SELECT 'recognition' AS date_role, toStartOfMonth(assumeNotNull(completed_date)) AS month, "
        "type, count(), uniqExact(order_id), sum(grand_total), sum(commission), "
        "sum(seller_total), sum(net_merchandise), sum(discount_amount), "
        f"sum(grand_total_refunded) FROM {fact} FINAL "
        "WHERE business_state = 'completed' AND completed_date IS NOT NULL "
        "GROUP BY month, type"
    )


def advance_watermark(ch, settings) -> datetime | None:
    fact = common.qualified(settings, common.FACT_TABLE)
    max_updated = common.as_naive(
        ch.query(f"SELECT max(updated_at) FROM {fact} FINAL").result_rows[0][0]
    )
    new_watermark = watermarking.advance_watermark(
        common.get_watermark(ch, settings), max_updated
    )
    if new_watermark is not None:
        common.set_watermark(ch, settings, new_watermark)
    return new_watermark


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Marketplace orders gated publish")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=("full", "incremental", "reconcile"), required=True)
    args = parser.parse_args(argv)

    common.setup_logging()
    log = common.LOGGER
    settings = load_settings()

    ch = common.clickhouse_client(settings)
    common.bootstrap_schema(ch, settings)

    controls = common.read_run_controls(ch, settings, args.run_id)
    if controls is None:
        raise RuntimeError(
            f"No run controls found for run_id={args.run_id}; the extract task "
            "of this DAG run did not record its snapshot totals."
        )

    build_tables(ch, settings)
    failures = run_publish_tests(ch, settings, controls, args.mode)
    if failures:
        raise RuntimeError(
            "Publish tests failed; published tables and watermark unchanged:\n- "
            + "\n- ".join(failures)
        )

    exchange_and_reload(ch, settings)
    new_watermark = advance_watermark(ch, settings)
    log.info(
        "Publish complete (mode=%s): %s active rows, watermark=%s",
        args.mode,
        controls.row_count,
        new_watermark,
    )


if __name__ == "__main__":
    main()
