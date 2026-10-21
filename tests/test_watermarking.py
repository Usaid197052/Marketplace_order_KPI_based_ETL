from datetime import datetime, timedelta

import pytest

from marketplace_etl.watermarking import (
    OVERLAP,
    advance_watermark,
    extraction_lower_bound,
    require_watermark,
)


def test_overlap_is_one_day():
    assert OVERLAP == timedelta(days=1)


def test_extraction_lower_bound_rewinds_one_day():
    watermark = datetime(2026, 10, 19, 11, 0, 0)
    assert extraction_lower_bound(watermark) == datetime(2026, 10, 18, 11, 0, 0)


def test_extraction_lower_bound_accepts_custom_overlap():
    watermark = datetime(2026, 10, 19, 11, 0, 0)
    assert extraction_lower_bound(watermark, timedelta(hours=2)) == datetime(
        2026, 10, 19, 9, 0, 0
    )


def test_advance_watermark_never_regresses():
    current = datetime(2026, 10, 19, 10, 0, 0)
    older = datetime(2026, 10, 18, 10, 0, 0)
    newer = datetime(2026, 10, 19, 12, 0, 0)
    assert advance_watermark(current, older) == current
    assert advance_watermark(current, newer) == newer
    assert advance_watermark(None, newer) == newer
    assert advance_watermark(current, None) == current
    assert advance_watermark(None, None) is None


def test_require_watermark_explains_missing_full_load():
    with pytest.raises(RuntimeError, match="full load"):
        require_watermark(None)
    watermark = datetime(2026, 10, 19, 11, 0, 0)
    assert require_watermark(watermark) is watermark
