CREATE TABLE IF NOT EXISTS marketplace.etl_watermarks
(
    pipeline   LowCardinality(String),
    watermark  DateTime,
    set_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (pipeline, set_at);

CREATE TABLE IF NOT EXISTS marketplace.etl_run_controls
(
    pipeline               LowCardinality(String),
    run_id                 String,
    source_row_count       UInt64,
    source_sum_grand_total  Decimal(38, 4),
    source_sum_commission   Decimal(38, 4),
    source_sum_seller_total Decimal(38, 4),
    source_max_updated_at  Nullable(DateTime),
    extracted_rows         UInt64,
    captured_at            DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(captured_at)
ORDER BY (pipeline, run_id);
