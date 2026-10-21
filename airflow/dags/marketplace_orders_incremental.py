from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from airflow.sdk import dag

from marketplace_spark import ensure_publish_pool, spark_job

ensure_publish_pool()


@dag(
    dag_id="marketplace_orders_incremental",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["marketplace", "etl", "incremental"],
)
def marketplace_orders_incremental():
    extract = spark_job(
        task_id="extract",
        script="incremental_load.py",
        application_args=["--run-id", "{{ run_id }}"],
        name="marketplace_orders_incremental",
    )
    publish = spark_job(
        task_id="publish",
        script="publish.py",
        application_args=["--run-id", "{{ run_id }}", "--mode", "incremental"],
        name="marketplace_orders_publish_incremental",
    )
    extract >> publish


marketplace_orders_incremental()
