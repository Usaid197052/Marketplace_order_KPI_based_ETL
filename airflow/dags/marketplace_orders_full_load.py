@dag(
    dag_id="marketplace_orders_full_load",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["marketplace", "etl", "full-load"],
)
def marketplace_orders_full_load():
    extract = spark_job(
        task_id="extract",
        script="full_load.py",
        application_args=["--run-id", "{{ run_id }}"],
        name="marketplace_orders_full_load",
    )
    publish = spark_job(
        task_id="publish",
        script="publish.py",
        application_args=["--run-id", "{{ run_id }}", "--mode", "full"],
        name="marketplace_orders_publish_full",
    )
    extract >> publish


marketplace_orders_full_load()
