from __future__ import annotations

from datetime import datetime
from decimal import Decimal


STATUS_MAP: dict[str, str] = {
    "pending_payment": "unpaid",
    "pending": "open",
    "processing": "open",
    "packed": "open",
    "shipped": "open",
    "completed": "completed",
    "canceled": "failed",
    "RTO": "failed",
    "returned": "returned",
    "partially_returned": "returned",
    "closed": "other",
    "migrated": "other",
    "finance_review": "other",
    "pending_payment_proof": "other",
}

DEFAULT_STATE = "other"


MIN_VALID_COMPLETED = datetime(2000, 1, 1)

_ZERO = Decimal("0")


def map_status(status: str | None) -> tuple[str, bool]:

    if status in STATUS_MAP:
        return STATUS_MAP[status], False
    return DEFAULT_STATE, True


def _dec(value) -> Decimal:
    return _ZERO if value is None else Decimal(value)


def net_merchandise(sub_total, discount_amount) -> Decimal:
    return _dec(sub_total) - _dec(discount_amount)


def take_rate(commission, sub_total) -> float | None:
    st = _dec(sub_total)
    if st == 0:
        return None
    return float(_dec(commission) / st)


def gmv_tie_out_gap(
    grand_total, sub_total, discount_amount, shipping_amount, cod_charges, mgmt_fee, vas_total
) -> Decimal:
    reconstructed = (
        _dec(sub_total)
        - _dec(discount_amount)
        + _dec(shipping_amount)
        + _dec(cod_charges)
        + _dec(mgmt_fee)
        + _dec(vas_total)
    )
    return _dec(grand_total) - reconstructed


def cycle_hours(created_at: datetime | None, completed_at: datetime | None) -> int | None:

    if created_at is None or completed_at is None or completed_at < MIN_VALID_COMPLETED:
        return None
    return int((completed_at - created_at).total_seconds() // 3600)


def is_quarantined(
    completed_at: datetime | None, cycle_h: int | None, gap: Decimal
) -> bool:
    if completed_at is not None and completed_at < MIN_VALID_COMPLETED:
        return True
    if cycle_h is not None and cycle_h < 0:
        return True
    return abs(gap) > 1


def compute_version(
    updated_at: datetime | None, created_at: datetime | None, extracted_at: datetime
) -> datetime:

    return updated_at or created_at or extracted_at


_DZ = "toDecimal64(0, 4)"


def business_state_case_sql(column: str = "status") -> str:
    whens = " ".join(
        f"WHEN {column} = '{raw}' THEN '{state}'" for raw, state in STATUS_MAP.items()
    )
    return f"CASE {whens} ELSE '{DEFAULT_STATE}' END"


def is_unmapped_status_sql(column: str = "status") -> str:
    known = ", ".join(f"'{raw}'" for raw in STATUS_MAP)
    return f"toUInt8({column} IS NULL OR {column} NOT IN ({known}))"


NET_MERCHANDISE_SQL = f"coalesce(sub_total, {_DZ}) - coalesce(discount_amount, {_DZ})"

TAKE_RATE_SQL = (
    f"if(coalesce(sub_total, {_DZ}) = {_DZ}, NULL, "
    f"toFloat64(coalesce(commission, {_DZ})) / toFloat64(coalesce(sub_total, {_DZ})))"
)

GMV_TIE_OUT_GAP_SQL = (
    f"coalesce(grand_total, {_DZ}) - ("
    f"coalesce(sub_total, {_DZ}) - coalesce(discount_amount, {_DZ}) "
    f"+ coalesce(shipping_amount, {_DZ}) + cod_charges + mgmt_fee + vas_total)"
)

CYCLE_HOURS_SQL = (
    "if(completed_at IS NOT NULL AND created_at IS NOT NULL "
    "AND completed_at >= toDateTime('2000-01-01'), "
    "dateDiff('hour', created_at, completed_at), NULL)"
)


IS_QUARANTINED_SQL = (
    "toUInt8(coalesce(completed_at < toDateTime('2000-01-01'), 0) "
    "OR coalesce(cycle_hours < 0, 0) OR abs(gmv_tie_out_gap) > 1)"
)

COMPLETED_DATE_SQL = (
    "if(completed_at IS NOT NULL AND completed_at >= toDateTime('2000-01-01'), "
    "toDate(completed_at), NULL)"
)


def staging_select_sql() -> str:

    dz = _DZ
    exprs = [
        "id",
        "marketplace_seller_id",
        "order_id",
        "old_order_id",
        "type",
        "status",
        f"{business_state_case_sql()} AS business_state",
        f"{is_unmapped_status_sql()} AS is_unmapped_status",
        f"coalesce(sub_total, {dz}) AS sub_total",
        f"coalesce(discount_amount, {dz}) AS discount_amount",
        f"coalesce(shipping_amount, {dz}) AS shipping_amount",
        "cod_charges",
        "mgmt_fee",
        "vas_total",
        f"coalesce(grand_total, {dz}) AS grand_total",
        f"coalesce(commission, {dz}) AS commission",
        f"coalesce(commission_percentage, {dz}) AS commission_percentage",
        f"coalesce(seller_total, {dz}) AS seller_total",
        "cashback_earned",
        f"coalesce(grand_total_refunded, {dz}) AS grand_total_refunded",
        f"{NET_MERCHANDISE_SQL} AS net_merchandise",
        f"{TAKE_RATE_SQL} AS take_rate",
        f"{GMV_TIE_OUT_GAP_SQL} AS gmv_tie_out_gap",
        f"{CYCLE_HOURS_SQL} AS cycle_hours",
        f"{IS_QUARANTINED_SQL} AS is_quarantined",
        "coalesce(total_item_count, 0) AS total_item_count",
        "coalesce(total_qty_ordered, 0) AS total_qty_ordered",
        "created_at",
        "updated_at",
        "completed_at",
        "expected_delivery_date",
        "returned_at",
        "toDate(created_at) AS created_date",
        f"{COMPLETED_DATE_SQL} AS completed_date",
        "_extracted_at",
        "_version",
    ]
    return ",\n    ".join(exprs)
