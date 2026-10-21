from marketplace_etl.common import split_sql_statements


def test_comment_semicolon_does_not_become_a_statement():
    sql = (
        "-- pipeline owns `ecommerce`; this pipeline never touches it.\n"
        "CREATE DATABASE IF NOT EXISTS marketplace;\n"
        "-- another comment\n"
        "CREATE TABLE IF NOT EXISTS marketplace.etl_watermarks (x UInt8) ENGINE = MergeTree ORDER BY x;\n"
    )
    statements = split_sql_statements(sql)
    assert statements == [
        "CREATE DATABASE IF NOT EXISTS marketplace",
        "CREATE TABLE IF NOT EXISTS marketplace.etl_watermarks (x UInt8) ENGINE = MergeTree ORDER BY x",
    ]
    assert all(not s.lower().startswith("this pipeline") for s in statements)
