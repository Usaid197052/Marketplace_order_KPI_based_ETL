from __future__ import annotations

from datetime import datetime, timedelta

OVERLAP = timedelta(days=1)


def extraction_lower_bound(watermark: datetime, overlap: timedelta = OVERLAP) -> datetime:
    return watermark - overlap


def advance_watermark(current: datetime | None, candidate: datetime | None) -> datetime | None:

    if current is None:
        return candidate
    if candidate is None:
        return current
    return max(current, candidate)


def require_watermark(watermark: datetime | None) -> datetime:
    if watermark is None:
        raise RuntimeError(
            "No watermark found for this pipeline. Run the one-time full load "
            "DAG (marketplace_orders_full_load) before the incremental or "
            "reconcile DAGs."
        )
    return watermark
