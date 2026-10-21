from __future__ import annotations

from decimal import Decimal

_DEC = "Nullable(Decimal(18, 4))"
_DECNN = "Decimal(18, 4)"
_TS = "Nullable(DateTime)"

SOURCE_COLUMNS: list[tuple[str, str, str]] = [
    ("id", "UInt64", "long"),
    ("old_order_id", "Nullable(Int64)", "long"),
    ("status", "LowCardinality(Nullable(String))", "string"),
    ("type", "LowCardinality(String)", "string"),
    ("is_withdrawal_requested", "UInt8", "int"),
    ("seller_payout_status", "LowCardinality(Nullable(String))", "string"),
    ("commission_percentage", _DEC, "decimal"),
    ("commission", _DEC, "decimal"),
    ("base_commission", _DEC, "decimal"),
    ("commission_invoiced", _DEC, "decimal"),
    ("base_commission_invoiced", _DEC, "decimal"),
    ("seller_total", _DEC, "decimal"),
    ("base_seller_total", _DEC, "decimal"),
    ("seller_total_invoiced", _DEC, "decimal"),
    ("base_seller_total_invoiced", _DEC, "decimal"),
    ("total_item_count", "Nullable(Int32)", "int"),
    ("total_qty_ordered", "Nullable(Int32)", "int"),
    ("grand_total", _DEC, "decimal"),
    ("cashback_earned", _DECNN, "decimal"),
    ("base_grand_total", _DEC, "decimal"),
    ("vas_total", _DECNN, "decimal"),
    ("grand_total_invoiced", _DEC, "decimal"),
    ("base_grand_total_invoiced", _DEC, "decimal"),
    ("grand_total_refunded", _DEC, "decimal"),
    ("base_grand_total_refunded", _DEC, "decimal"),
    ("sub_total", _DEC, "decimal"),
    ("base_sub_total", _DEC, "decimal"),
    ("wallet_credit_used", _DECNN, "decimal"),
    ("sub_total_invoiced", _DEC, "decimal"),
    ("base_sub_total_invoiced", _DEC, "decimal"),
    ("sub_total_refunded", _DEC, "decimal"),
    ("base_sub_total_refunded", _DEC, "decimal"),
    ("discount_percent", _DEC, "decimal"),
    ("discount_amount", _DEC, "decimal"),
    ("base_discount_amount", _DEC, "decimal"),
    ("discount_amount_invoiced", _DEC, "decimal"),
    ("base_discount_amount_invoiced", _DEC, "decimal"),
    ("discount_refunded", _DEC, "decimal"),
    ("base_discount_refunded", _DEC, "decimal"),
    ("tax_amount", _DEC, "decimal"),
    ("base_tax_amount", _DEC, "decimal"),
    ("tax_amount_invoiced", _DEC, "decimal"),
    ("base_tax_amount_invoiced", _DEC, "decimal"),
    ("tax_amount_refunded", _DEC, "decimal"),
    ("base_tax_amount_refunded", _DEC, "decimal"),
    ("shipping_amount", _DEC, "decimal"),
    ("base_shipping_amount", _DEC, "decimal"),
    ("shipping_invoiced", _DEC, "decimal"),
    ("base_shipping_invoiced", _DEC, "decimal"),
    ("shipping_refunded", _DEC, "decimal"),
    ("base_shipping_refunded", _DEC, "decimal"),
    ("sub_total_incl_tax", _DECNN, "decimal"),
    ("base_sub_total_incl_tax", _DECNN, "decimal"),
    ("shipping_amount_incl_tax", _DECNN, "decimal"),
    ("base_shipping_amount_incl_tax", _DECNN, "decimal"),
    ("cod_charges", _DECNN, "decimal"),
    ("mgmt_fee", _DECNN, "decimal"),
    ("marketplace_seller_id", "UInt64", "long"),
    ("order_id", "UInt64", "long"),
    ("created_at", _TS, "timestamp"),
    ("updated_at", _TS, "timestamp"),
    ("completed_at", _TS, "timestamp"),
    ("expected_delivery_date", _TS, "timestamp"),
    ("returned_at", _TS, "timestamp"),
]

METADATA_COLUMNS: list[tuple[str, str]] = [
    ("_extracted_at", "DateTime"),
    ("_is_deleted", "UInt8"),
    ("_deleted_at", "Nullable(DateTime)"),
    ("_version", "DateTime64(3)"),
]

SOURCE_COLUMN_NAMES: list[str] = [name for name, _, _ in SOURCE_COLUMNS]
ALL_RAW_COLUMN_NAMES: list[str] = SOURCE_COLUMN_NAMES + [n for n, _ in METADATA_COLUMNS]

_DECIMAL_INDEXES = [i for i, (_, _, kind) in enumerate(SOURCE_COLUMNS) if kind == "decimal"]
_QUANT = Decimal("0.0001")


STAGING_COLUMN_NAMES: list[str] = [
    "id",
    "marketplace_seller_id",
    "order_id",
    "old_order_id",
    "type",
    "status",
    "business_state",
    "is_unmapped_status",
    "sub_total",
    "discount_amount",
    "shipping_amount",
    "cod_charges",
    "mgmt_fee",
    "vas_total",
    "grand_total",
    "commission",
    "commission_percentage",
    "seller_total",
    "cashback_earned",
    "grand_total_refunded",
    "net_merchandise",
    "take_rate",
    "gmv_tie_out_gap",
    "cycle_hours",
    "is_quarantined",
    "total_item_count",
    "total_qty_ordered",
    "created_at",
    "updated_at",
    "completed_at",
    "expected_delivery_date",
    "returned_at",
    "created_date",
    "completed_date",
    "_extracted_at",
    "_version",
]


def prepare_source_row(row: tuple) -> tuple:

    values = list(row)
    for i in _DECIMAL_INDEXES:
        if values[i] is not None:
            values[i] = Decimal(values[i]).quantize(_QUANT)
    return tuple(values)


def spark_source_schema():

    from pyspark.sql import types as T

    kind_map = {
        "long": T.LongType(),
        "int": T.IntegerType(),
        "string": T.StringType(),
        "decimal": T.DecimalType(18, 4),
        "timestamp": T.TimestampType(),
    }
    return T.StructType(
        [T.StructField(name, kind_map[kind], nullable=True) for name, _, kind in SOURCE_COLUMNS]
    )
