CREATE TABLE IF NOT EXISTS marketplace.kpi_monthly
(
    date_role        LowCardinality(String),
    month            Date,
    type             LowCardinality(String),
    rows_count       SimpleAggregateFunction(sum, UInt64),
    order_count      SimpleAggregateFunction(max, UInt64),
    gmv              SimpleAggregateFunction(sum, Decimal(38, 4)),
    commission       SimpleAggregateFunction(sum, Decimal(38, 4)),
    seller_payable   SimpleAggregateFunction(sum, Decimal(38, 4)),
    net_merchandise  SimpleAggregateFunction(sum, Decimal(38, 4)),
    discount_amount  SimpleAggregateFunction(sum, Decimal(38, 4)),
    refunded         SimpleAggregateFunction(sum, Decimal(38, 4))
)
ENGINE = AggregatingMergeTree
ORDER BY (date_role, month, type);
