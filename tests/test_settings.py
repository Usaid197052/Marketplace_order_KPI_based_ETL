import pytest

from marketplace_etl.schema import SOURCE_COLUMNS, SOURCE_COLUMN_NAMES, STAGING_COLUMN_NAMES
from marketplace_etl.settings import CLICKHOUSE_DATABASE, parse_mariadb_url


def test_clickhouse_database_is_marketplace_not_ecommerce():
    assert CLICKHOUSE_DATABASE == "marketplace"


def test_parse_mariadb_url_splits_sqlalchemy_style_dsn():
    parsed = parse_mariadb_url(
        "mysql+pymysql://example_user:example_secret@db.example:3307/cartlow_prod"
    )
    assert parsed.host == "db.example"
    assert parsed.port == 3307
    assert parsed.user == "example_user"
    assert parsed.password == "example_secret"
    assert parsed.database == "cartlow_prod"


def test_parse_mariadb_url_rejects_unusable_values():
    with pytest.raises(ValueError):
        parse_mariadb_url("not-a-url")


def test_source_column_inventory_is_the_full_64_column_mirror():
    assert len(SOURCE_COLUMNS) == 64
    assert SOURCE_COLUMN_NAMES[0] == "id"
    assert SOURCE_COLUMN_NAMES[-1] == "returned_at"
    assert "grand_total" in SOURCE_COLUMN_NAMES
    assert "marketplace_seller_id" in SOURCE_COLUMN_NAMES

    assert "grand_total_invoiced" in SOURCE_COLUMN_NAMES
    assert "grand_total_invoiced" not in STAGING_COLUMN_NAMES
    assert "tax_amount" not in STAGING_COLUMN_NAMES
    assert "wallet_credit_used" not in STAGING_COLUMN_NAMES
    assert "base_grand_total" not in STAGING_COLUMN_NAMES
    assert "business_state" in STAGING_COLUMN_NAMES
    assert "gmv_tie_out_gap" in STAGING_COLUMN_NAMES
    assert "is_quarantined" in STAGING_COLUMN_NAMES
