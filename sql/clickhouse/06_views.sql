CREATE OR REPLACE VIEW marketplace.vw_certified_revenue AS
SELECT
    toStartOfMonth(assumeNotNull(completed_date))                   AS month,
    type,
    count()                                                         AS row_count,
    uniqExact(order_id)                                             AS order_count,
    sum(grand_total)                                                AS realized_gmv,
    sum(commission)                                                 AS platform_commission,
    sum(seller_total)                                               AS seller_payable,
    sum(net_merchandise)                                            AS net_merchandise,
    toFloat64(sum(grand_total)) / nullIf(toFloat64(uniqExact(order_id)), 0)
                                                                    AS completed_aov,
    toFloat64(sum(commission)) / nullIf(toFloat64(sum(sub_total)), 0)
                                                                    AS take_rate_on_subtotal,
    toFloat64(sum(commission)) / nullIf(toFloat64(sum(grand_total)), 0)
                                                                    AS take_rate_on_gmv
FROM marketplace.fact_marketplace_order FINAL
WHERE business_state = 'completed' AND completed_date IS NOT NULL
GROUP BY month, type;

CREATE OR REPLACE VIEW marketplace.vw_checkout_funnel AS
SELECT
    toStartOfMonth(assumeNotNull(created_date))                     AS month,
    count()                                                         AS row_count,
    uniqExact(order_id)                                             AS order_count,
    sum(grand_total)                                                AS booked_checkout_value,
    countIf(business_state = 'unpaid')                              AS unpaid_rows,
    toFloat64(countIf(business_state = 'unpaid')) / nullIf(toFloat64(count()), 0)
                                                                    AS unpaid_rate,
    countIf(business_state != 'unpaid')                             AS paid_rows,
    countIf(business_state = 'completed')                           AS completed_rows,
    toFloat64(countIf(business_state = 'completed'))
        / nullIf(toFloat64(countIf(business_state != 'unpaid')), 0) AS paid_completion_rate,
    countIf(business_state = 'failed')                              AS failed_rows,
    toFloat64(countIf(business_state = 'failed'))
        / nullIf(toFloat64(countIf(business_state != 'unpaid')), 0) AS fail_rate
FROM marketplace.fact_marketplace_order FINAL
WHERE created_date IS NOT NULL
GROUP BY month;

CREATE OR REPLACE VIEW marketplace.vw_unpaid_aging AS
SELECT
    multiIf(
        age_days <= 1,  '0-1d',
        age_days <= 7,  '2-7d',
        age_days <= 30, '8-30d',
        age_days <= 90, '31-90d',
        '90d+'
    )                   AS bucket,
    min(age_days)       AS bucket_sort,
    count()             AS row_count,
    sum(grand_total)    AS booked_value
FROM
(
    SELECT
        dateDiff('day', assumeNotNull(created_at), now()) AS age_days,
        grand_total
    FROM marketplace.fact_marketplace_order FINAL
    WHERE business_state = 'unpaid' AND created_at IS NOT NULL
)
GROUP BY bucket;

CREATE OR REPLACE VIEW marketplace.vw_in_flight AS
SELECT
    type,
    status,
    count()          AS row_count,
    sum(grand_total) AS open_value
FROM marketplace.fact_marketplace_order FINAL
WHERE business_state = 'open'
GROUP BY type, status;

CREATE OR REPLACE VIEW marketplace.vw_returns AS
SELECT
    toStartOfMonth(assumeNotNull(created_date))                     AS month,
    countIf(business_state = 'completed')                           AS completed_rows,
    countIf(business_state = 'returned')                            AS returned_rows,
    toFloat64(countIf(business_state = 'returned'))
        / nullIf(toFloat64(countIf(business_state IN ('completed', 'returned'))), 0)
                                                                    AS return_rate,
    countIf(business_state = 'returned' AND returned_at IS NULL)    AS returned_missing_timestamp,
    sum(grand_total_refunded)                                       AS cash_refunded
FROM marketplace.fact_marketplace_order FINAL
WHERE created_date IS NOT NULL
GROUP BY month;

CREATE OR REPLACE VIEW marketplace.vw_cycle_time AS
SELECT
    toStartOfMonth(assumeNotNull(completed_date))   AS month,
    type,
    count()                                         AS completed_rows,
    avg(cycle_hours)                                AS avg_cycle_hours,
    toFloat64(countIf(cycle_hours < 24)) / nullIf(toFloat64(count()), 0)
                                                    AS share_under_one_day
FROM marketplace.fact_marketplace_order FINAL
WHERE business_state = 'completed'
  AND completed_date IS NOT NULL
  AND is_quarantined = 0
GROUP BY month, type;

CREATE OR REPLACE VIEW marketplace.vw_on_time_physical AS
SELECT
    countIf(business_state = 'open')                                AS in_flight_rows,
    countIf(business_state = 'open'
            AND expected_delivery_date IS NOT NULL
            AND expected_delivery_date < now())                     AS in_flight_past_due,
    countIf(business_state = 'completed'
            AND expected_delivery_date IS NOT NULL)                 AS completed_with_expected_date,
    countIf(business_state = 'completed'
            AND expected_delivery_date IS NOT NULL
            AND completed_at <= expected_delivery_date)             AS completed_on_time,
    toFloat64(countIf(business_state = 'completed'
                      AND expected_delivery_date IS NOT NULL
                      AND completed_at <= expected_delivery_date))
        / nullIf(toFloat64(countIf(business_state = 'completed'
                                   AND expected_delivery_date IS NOT NULL)), 0)
                                                                    AS completed_on_time_rate
FROM marketplace.fact_marketplace_order FINAL
WHERE type = 'physical_items';

CREATE OR REPLACE VIEW marketplace.vw_active_sellers AS
SELECT
    toStartOfMonth(assumeNotNull(completed_date)) AS month,
    uniqExact(marketplace_seller_id)              AS active_sellers
FROM marketplace.fact_marketplace_order FINAL
WHERE business_state = 'completed' AND completed_date IS NOT NULL
GROUP BY month;

CREATE OR REPLACE VIEW marketplace.vw_seller_concentration AS
WITH ranked AS
(
    SELECT
        marketplace_seller_id,
        gmv,
        row_number() OVER (ORDER BY gmv DESC) AS rn
    FROM
    (
        SELECT marketplace_seller_id, sum(grand_total) AS gmv
        FROM marketplace.fact_marketplace_order FINAL
        WHERE business_state = 'completed'
          AND type = 'physical_items'
          AND completed_date >= toDate(now()) - INTERVAL 12 MONTH
        GROUP BY marketplace_seller_id
    )
)
SELECT
    count()                                                        AS seller_count,
    sum(gmv)                                                       AS total_gmv,
    toFloat64(sumIf(gmv, rn <= 5))  / nullIf(toFloat64(sum(gmv)), 0) AS top5_share,
    toFloat64(sumIf(gmv, rn <= 10)) / nullIf(toFloat64(sum(gmv)), 0) AS top10_share,
    toFloat64(sumIf(gmv, rn <= 20)) / nullIf(toFloat64(sum(gmv)), 0) AS top20_share
FROM ranked;

CREATE OR REPLACE VIEW marketplace.vw_discount_fee_bridge AS
SELECT
    toStartOfMonth(assumeNotNull(created_date))                       AS month,
    count()                                                           AS row_count,
    toFloat64(sum(discount_amount)) / nullIf(toFloat64(sum(sub_total)), 0)
                                                                      AS discount_share_of_subtotal,
    toFloat64(countIf(shipping_amount > 0)) / nullIf(toFloat64(count()), 0)
                                                                      AS shipping_attachment,
    toFloat64(countIf(mgmt_fee > 0)) / nullIf(toFloat64(count()), 0)  AS mgmt_fee_attachment,
    toFloat64(countIf(cod_charges > 0)) / nullIf(toFloat64(count()), 0)
                                                                      AS cod_attachment,
    toFloat64(countIf(vas_total > 0)) / nullIf(toFloat64(count()), 0) AS vas_attachment
FROM marketplace.fact_marketplace_order FINAL
WHERE business_state = 'completed' AND created_date IS NOT NULL
GROUP BY month;

CREATE OR REPLACE VIEW marketplace.vw_data_quality AS
SELECT
    count()                                                           AS active_rows,
    countIf(is_quarantined = 1)                                       AS quarantined_rows,
    countIf(abs(gmv_tie_out_gap) > 1)                                 AS tie_out_gap_rows,
    countIf(is_unmapped_status = 1)                                   AS unmapped_status_rows,
    countIf(business_state = 'returned' AND returned_at IS NULL)      AS returned_missing_timestamp,
    countIf(completed_at IS NOT NULL
            AND completed_at < toDateTime('2000-01-01'))              AS completed_before_2000,
    countIf(cycle_hours < 0)                                          AS negative_cycle_rows,
    max(updated_at)                                                   AS max_updated_at
FROM marketplace.fact_marketplace_order FINAL;
