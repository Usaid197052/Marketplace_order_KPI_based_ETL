from pathlib import Path

import marketplace_etl.common as common

ROOT = Path(__file__).resolve().parents[1]
JOBS = ROOT / "marketplace_etl" / "jobs"
COMMON = ROOT / "marketplace_etl" / "common.py"

_WRITE_MARKERS = (
    'cur.execute("INSERT',
    "cur.execute('INSERT",
    'cur.execute("UPDATE',
    "cur.execute('UPDATE",
    'cur.execute("DELETE',
    "cur.execute('DELETE",
    'cur.execute("CREATE',
    "cur.execute('CREATE",
    'cur.execute("ALTER',
    "cur.execute('ALTER",
    'cur.execute("DROP',
    "cur.execute('DROP",
    ".write.jdbc",
    'format("jdbc")',
)


def test_source_session_is_read_only_consistent_snapshot():
    text = COMMON.read_text(encoding="utf-8")
    assert "SET SESSION TRANSACTION READ ONLY" in text
    assert "START TRANSACTION WITH CONSISTENT SNAPSHOT" in text
    assert "autocommit=False" in text


def test_python_never_issues_a_write_against_the_source_cursor():
    files = [COMMON, *JOBS.glob("*.py")]
    for path in files:
        text = path.read_text(encoding="utf-8")
        for marker in _WRITE_MARKERS:
            assert marker not in text, f"{path.name} contains {marker!r}"


def test_open_source_snapshot_documents_read_only():
    source = common.open_source_snapshot.__doc__ or ""
    assert "read-only" in source.lower()
