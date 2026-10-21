from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Sequence

from marketplace_etl import schema
from marketplace_etl.settings import PIPELINE, Settings

LOGGER = logging.getLogger("marketplace_etl")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql" / "clickhouse"


BOOTSTRAP_FILES = (
    "00_database.sql",
    "01_watermarks.sql",
    "02_raw.sql",
    "03_staging.sql",
    "04_fact.sql",
    "05_dim_date.sql",
    "06_views.sql",
    "07_kpi_monthly.sql",
)

RAW_TABLE = "raw_marketplace_orders"
STG_TABLE = "stg_marketplace_orders"
STG_BUILD_TABLE = "stg_marketplace_orders_build"
FACT_TABLE = "fact_marketplace_order"
FACT_BUILD_TABLE = "fact_marketplace_order_build"
KPI_TABLE = "kpi_monthly"
WATERMARKS_TABLE = "etl_watermarks"
CONTROLS_TABLE = "etl_run_controls"

_SELECT_COLUMNS = ", ".join(schema.SOURCE_COLUMN_NAMES)


def setup_logging() -> None:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def as_naive(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def chunked(items: Sequence, size: int) -> Iterator[Sequence]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def clickhouse_client(settings: Settings):
    import clickhouse_connect


    return clickhouse_connect.get_client(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        username=settings.clickhouse.user,
        password=settings.clickhouse.password,
        database="default",
    )


def qualified(settings: Settings, table: str) -> str:
    return f"{settings.clickhouse.database}.{table}"


def split_sql_statements(text: str) -> list[str]:

    kept: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        kept.append(line)
    return [part.strip() for part in "\n".join(kept).split(";") if part.strip()]


def bootstrap_schema(client, settings: Settings) -> None:

    for file_name in BOOTSTRAP_FILES:
        sql_text = (SQL_DIR / file_name).read_text(encoding="utf-8")
        for statement in split_sql_statements(sql_text):
            client.command(statement)
    LOGGER.info("ClickHouse schema bootstrap complete (%d files)", len(BOOTSTRAP_FILES))


def open_source_snapshot(settings: Settings):

    import pymysql

    conn = pymysql.connect(
        host=settings.mariadb.host,
        port=settings.mariadb.port,
        user=settings.mariadb.user,
        password=settings.mariadb.password,
        database=settings.mariadb.database,
        autocommit=False,
        charset="utf8mb4",
        connect_timeout=30,
        read_timeout=600,
    )
    with conn.cursor() as cur:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
        cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    return conn


def close_source(conn) -> None:
    try:
        conn.rollback()
    finally:
        conn.close()


@dataclass(frozen=True)
class SourceControls:
    row_count: int
    sum_grand_total: Decimal
    sum_commission: Decimal
    sum_seller_total: Decimal
    max_updated_at: datetime | None


def capture_source_controls(conn, settings: Settings) -> SourceControls:
    sql = (
        "SELECT COUNT(*), COALESCE(SUM(grand_total), 0), COALESCE(SUM(commission), 0), "
        f"COALESCE(SUM(seller_total), 0), MAX(updated_at) FROM {settings.source_table}"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        count, g, c, s, max_updated = cur.fetchone()
    return SourceControls(
        row_count=int(count),
        sum_grand_total=Decimal(g),
        sum_commission=Decimal(c),
        sum_seller_total=Decimal(s),
        max_updated_at=max_updated,
    )


def iter_full_pages(conn, settings: Settings) -> Iterator[list[tuple]]:

    last_id = 0
    sql = (
        f"SELECT {_SELECT_COLUMNS} FROM {settings.source_table} "
        "WHERE id > %s ORDER BY id LIMIT %s"
    )
    while True:
        with conn.cursor() as cur:
            cur.execute(sql, (last_id, settings.full_load_page_size))
            rows = cur.fetchall()
        if not rows:
            return
        last_id = rows[-1][0]
        yield list(rows)


def fetch_incremental_rows(conn, settings: Settings, lower_bound: datetime) -> list[tuple]:

    sql = (
        f"SELECT {_SELECT_COLUMNS} FROM {settings.source_table} "
        "WHERE updated_at >= %s ORDER BY id"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (lower_bound,))
        return list(cur.fetchall())


def iter_source_ids(conn, settings: Settings) -> Iterator[list[int]]:

    last_id = 0
    sql = (
        f"SELECT id FROM {settings.source_table} "
        "WHERE id > %s ORDER BY id LIMIT %s"
    )
    while True:
        with conn.cursor() as cur:
            cur.execute(sql, (last_id, settings.delete_scan_page_size))
            rows = cur.fetchall()
        if not rows:
            return
        last_id = rows[-1][0]
        yield [r[0] for r in rows]


def build_spark(app_name: str):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def rows_to_raw_tuples(spark, rows: Sequence[tuple], extracted_at: datetime) -> list[tuple]:

    from pyspark.sql import functions as F

    prepared = [schema.prepare_source_row(r) for r in rows]
    df = spark.createDataFrame(prepared, schema.spark_source_schema())
    df = (
        df.withColumn("_extracted_at", F.lit(extracted_at))
        .withColumn("_is_deleted", F.lit(0))
        .withColumn("_deleted_at", F.lit(None).cast("timestamp"))
        .withColumn(
            "_version",
            F.coalesce(F.col("updated_at"), F.col("created_at"), F.lit(extracted_at)),
        )
        .select(*schema.ALL_RAW_COLUMN_NAMES)
    )
    return [tuple(row) for row in df.collect()]


def insert_raw_rows(client, settings: Settings, tuples: Sequence[tuple]) -> None:
    table = qualified(settings, RAW_TABLE)
    for chunk in chunked(tuples, settings.insert_batch_size):
        client.insert(table, list(chunk), column_names=schema.ALL_RAW_COLUMN_NAMES)


def store_run_controls(
    client,
    settings: Settings,
    run_id: str,
    controls: SourceControls,
    extracted_rows: int,
) -> None:
    client.insert(
        qualified(settings, CONTROLS_TABLE),
        [[
            PIPELINE,
            run_id,
            controls.row_count,
            controls.sum_grand_total,
            controls.sum_commission,
            controls.sum_seller_total,
            controls.max_updated_at,
            extracted_rows,
        ]],
        column_names=[
            "pipeline",
            "run_id",
            "source_row_count",
            "source_sum_grand_total",
            "source_sum_commission",
            "source_sum_seller_total",
            "source_max_updated_at",
            "extracted_rows",
        ],
    )
    LOGGER.info(
        "Stored run controls run_id=%s rows=%s max_updated_at=%s",
        run_id,
        controls.row_count,
        controls.max_updated_at,
    )


def read_run_controls(client, settings: Settings, run_id: str) -> SourceControls | None:
    result = client.query(
        "SELECT source_row_count, source_sum_grand_total, source_sum_commission, "
        "source_sum_seller_total, source_max_updated_at "
        f"FROM {qualified(settings, CONTROLS_TABLE)} FINAL "
        "WHERE pipeline = {pipeline:String} AND run_id = {run_id:String} "
        "ORDER BY captured_at DESC LIMIT 1",
        parameters={"pipeline": PIPELINE, "run_id": run_id},
    )
    if not result.result_rows:
        return None
    count, g, c, s, max_updated = result.result_rows[0]
    return SourceControls(
        row_count=int(count),
        sum_grand_total=Decimal(g),
        sum_commission=Decimal(c),
        sum_seller_total=Decimal(s),
        max_updated_at=as_naive(max_updated),
    )


def get_watermark(client, settings: Settings) -> datetime | None:
    result = client.query(
        f"SELECT watermark FROM {qualified(settings, WATERMARKS_TABLE)} "
        "WHERE pipeline = {pipeline:String} ORDER BY watermark DESC LIMIT 1",
        parameters={"pipeline": PIPELINE},
    )
    if not result.result_rows:
        return None
    return as_naive(result.result_rows[0][0])


def set_watermark(client, settings: Settings, value: datetime) -> None:
    client.insert(
        qualified(settings, WATERMARKS_TABLE),
        [[PIPELINE, value]],
        column_names=["pipeline", "watermark"],
    )
    LOGGER.info("Watermark set to %s", value)
