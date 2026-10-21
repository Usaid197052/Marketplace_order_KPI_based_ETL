from datetime import datetime
from decimal import Decimal

from marketplace_etl.transforms import (
    GMV_TIE_OUT_GAP_SQL,
    STATUS_MAP,
    business_state_case_sql,
    cycle_hours,
    gmv_tie_out_gap,
    is_quarantined,
    is_unmapped_status_sql,
    map_status,
    net_merchandise,
    staging_select_sql,
    take_rate,
)


def test_status_map_covers_every_documented_raw_status():
    expected = {
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
    assert STATUS_MAP == expected


def test_map_status_known_and_unknown():
    assert map_status("completed") == ("completed", False)
    assert map_status("pending_payment") == ("unpaid", False)
    assert map_status("RTO") == ("failed", False)
    assert map_status("mystery") == ("other", True)
    assert map_status(None) == ("other", True)
    assert map_status("") == ("other", True)


def test_net_merchandise_subtracts_discount_and_treats_null_as_zero():
    assert net_merchandise(Decimal("100.00"), Decimal("15.50")) == Decimal("84.50")
    assert net_merchandise(None, Decimal("5")) == Decimal("-5")
    assert net_merchandise(Decimal("10"), None) == Decimal("10")


def test_take_rate_is_none_when_sub_total_is_zero():
    assert take_rate(Decimal("20"), Decimal("100")) == 0.2
    assert take_rate(Decimal("20"), Decimal("0")) is None
    assert take_rate(Decimal("20"), None) is None
    assert take_rate(None, Decimal("50")) == 0.0


def test_gmv_tie_out_gap_matches_spec_identity():

    gap = gmv_tie_out_gap(
        grand_total=Decimal("110"),
        sub_total=Decimal("100"),
        discount_amount=Decimal("5"),
        shipping_amount=Decimal("10"),
        cod_charges=Decimal("2"),
        mgmt_fee=Decimal("3"),
        vas_total=Decimal("0"),
    )
    assert gap == Decimal("0")

    off_by_two = gmv_tie_out_gap(
        grand_total=Decimal("112"),
        sub_total=Decimal("100"),
        discount_amount=Decimal("5"),
        shipping_amount=Decimal("10"),
        cod_charges=Decimal("2"),
        mgmt_fee=Decimal("3"),
        vas_total=Decimal("0"),
    )
    assert off_by_two == Decimal("2")


def test_cycle_hours_none_when_completed_before_2000():
    created = datetime(2024, 1, 1, 12, 0, 0)
    completed = datetime(2024, 1, 3, 14, 30, 0)
    assert cycle_hours(created, completed) == 50
    assert cycle_hours(created, datetime(1970, 1, 1)) is None
    assert cycle_hours(created, None) is None
    assert cycle_hours(None, completed) is None


def test_cycle_hours_negative_when_completed_before_created():
    created = datetime(2024, 1, 3, 12, 0, 0)
    completed = datetime(2024, 1, 1, 12, 0, 0)
    assert cycle_hours(created, completed) == -48


def test_quarantine_on_epoch_completed_at():
    assert is_quarantined(datetime(1970, 1, 1), None, Decimal("0")) is True


def test_quarantine_on_negative_cycle():
    assert is_quarantined(datetime(2024, 1, 1), -1, Decimal("0")) is True


def test_quarantine_on_money_gap_over_one():
    assert is_quarantined(datetime(2024, 1, 1), 5, Decimal("1.01")) is True
    assert is_quarantined(datetime(2024, 1, 1), 5, Decimal("-1.01")) is True


def test_quarantine_does_not_fire_on_gap_of_one_or_healthy_row():
    assert is_quarantined(datetime(2024, 1, 1), 5, Decimal("1")) is False
    assert is_quarantined(datetime(2024, 1, 1), 5, Decimal("0.4")) is False
    assert is_quarantined(None, None, Decimal("0")) is False


def test_sql_builders_stay_aligned_with_status_map():
    case_sql = business_state_case_sql()
    unmapped_sql = is_unmapped_status_sql()
    for raw, state in STATUS_MAP.items():
        assert f"WHEN status = '{raw}' THEN '{state}'" in case_sql
        assert f"'{raw}'" in unmapped_sql
    assert "ELSE 'other'" in case_sql


def test_staging_select_includes_derived_columns_and_active_raw_source():
    sql = staging_select_sql()
    assert "AS business_state" in sql
    assert "AS net_merchandise" in sql
    assert "AS take_rate" in sql
    assert "AS gmv_tie_out_gap" in sql
    assert "AS cycle_hours" in sql
    assert "AS is_quarantined" in sql
    assert "AS created_date" in sql
    assert "AS completed_date" in sql
    assert "sub_total" in GMV_TIE_OUT_GAP_SQL
    assert "cod_charges" in GMV_TIE_OUT_GAP_SQL
