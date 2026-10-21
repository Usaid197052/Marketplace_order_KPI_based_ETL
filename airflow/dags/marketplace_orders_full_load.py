        application_args=["--run-id", "{{ run_id }}", "--mode", "full"],
        name="marketplace_orders_publish_full",
    )
    extract >> publish


marketplace_orders_full_load()
