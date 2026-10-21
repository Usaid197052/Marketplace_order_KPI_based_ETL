CREATE TABLE IF NOT EXISTS marketplace.dim_date
(
    date        Date,
    year        UInt16,
    quarter     UInt8,
    month       UInt8,
    month_name  LowCardinality(String),
    iso_week    UInt8,
    day         UInt8,
    day_name    LowCardinality(String),
    is_weekend  UInt8
)
ENGINE = MergeTree
ORDER BY date;

INSERT INTO marketplace.dim_date
SELECT
    d                             AS date,
    toYear(d)                     AS year,
    toQuarter(d)                  AS quarter,
    toMonth(d)                    AS month,
    dateName('month', d)          AS month_name,
    toISOWeek(d)                  AS iso_week,
    toDayOfMonth(d)               AS day,
    dateName('weekday', d)        AS day_name,
    toUInt8(toDayOfWeek(d) >= 6)  AS is_weekend
FROM
(
    SELECT toDate('2020-01-01') + number AS d
    FROM numbers(4018)
)
WHERE (SELECT count() FROM marketplace.dim_date) = 0;
