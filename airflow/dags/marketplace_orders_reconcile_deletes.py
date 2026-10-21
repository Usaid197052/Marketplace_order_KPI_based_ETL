from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from airflow.sdk import dag

from marketplace_spark import ensure_publish_pool, spark_job

ensure_publish_pool()


@dag(
    dag_id="marketplace_orders_reconcile_deletes",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["marketplace", "etl", "deletes"],
)
def marketplace_orders_reconcile_deletes():
    reconcile = spark_job(
        task_id="reconcile",
        script="reconcile_deletes.py",
        application_args=["--run-id", "{{ run_id }}"],
        name="marketplace_orders_reconcile_deletes",
    )
    publish = spark_job(
        task_id="publish",
        script="publish.py",
        application_args=["--run-id", "{{ run_id }}", "--mode", "reconcile"],
        name="marketplace_orders_publish_reconcile",
    )
    reconcile >> publish


marketplace_orders_reconcile_deletes()
