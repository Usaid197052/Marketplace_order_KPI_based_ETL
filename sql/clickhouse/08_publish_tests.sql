SELECT count() FROM marketplace.fact_marketplace_order_build;

SELECT sum(grand_total), sum(commission), sum(seller_total)
FROM marketplace.fact_marketplace_order_build;

SELECT count() FROM (
    SELECT marketplace_seller_id, order_id
    FROM marketplace.fact_marketplace_order_build
    GROUP BY marketplace_seller_id, order_id
    HAVING count() > 1
);

SELECT countIf(abs(gmv_tie_out_gap) > 1)
FROM marketplace.fact_marketplace_order_build;

SELECT create_table_query FROM system.tables
WHERE database = 'marketplace' AND name = 'vw_certified_revenue';

SELECT max(updated_at) FROM marketplace.fact_marketplace_order_build;
