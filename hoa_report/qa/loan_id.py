from __future__ import annotations

import re
from collections import Counter
from typing import Any

_NON_ALNUM = re.compile(r"[^A-Za-z0-9]+")


def normalize_loan_id(x: Any) -> str | None:
    """Normalize loan identifiers into a canonical deterministic format."""
    if x is None:
        return None

    # NaN check without importing pandas/numpy.
    if isinstance(x, float) and x != x:
        return None

    normalized = str(x)
    normalized = normalized.strip()
    normalized = re.sub(r"\.0$", "", normalized)
    normalized = _NON_ALNUM.sub("", normalized)
    normalized = normalized.upper()

    if not normalized:
        return None
    return normalized


def _find_duplicate_ids_records(records: list[dict[str, Any]], id_col: str) -> list[dict[str, Any]]:
    if any(id_col not in record for record in records):
        raise KeyError(f"Column '{id_col}' not found in record(s)")

    normalized_ids = [normalize_loan_id(record[id_col]) for record in records]
    counts = Counter(value for value in normalized_ids if value is not None)

    duplicates: list[dict[str, Any]] = []
    for record, normalized in zip(records, normalized_ids, strict=True):
        if normalized is not None and counts[normalized] > 1:
            row = dict(record)
            row["normalized_loan_id"] = normalized
            duplicates.append(row)
    return duplicates


def find_duplicate_ids(df: Any, id_col: str) -> Any:
    """Return rows with duplicate normalized IDs for reporting workflows."""
    if isinstance(df, list):
        return _find_duplicate_ids_records(df, id_col)

    if not hasattr(df, "columns") or id_col not in df.columns:
        raise KeyError(f"Column '{id_col}' not found in DataFrame")

    normalized_ids = df[id_col].map(normalize_loan_id)
    mask = normalized_ids.notna() & normalized_ids.duplicated(keep=False)

    duplicates = df.loc[mask].copy()
    duplicates["normalized_loan_id"] = normalized_ids.loc[mask]
    return duplicates


def assert_unique_vendor_ids(df: Any, id_col: str) -> None:
    """Raise an exception when vendor IDs are not unique after normalization."""
    duplicate_rows = find_duplicate_ids(df, id_col)

    if isinstance(duplicate_rows, list):
        if not duplicate_rows:
            return
        counts = Counter(row["normalized_loan_id"] for row in duplicate_rows)
    else:
        if duplicate_rows.empty:
            return
        counts = Counter(duplicate_rows["normalized_loan_id"].tolist())

    duplicate_summary = ", ".join(
        f"{loan_id} ({count} rows)" for loan_id, count in sorted(counts.items())
    )
    raise ValueError(
        "Duplicate vendor loan IDs detected after normalization in "
        f"'{id_col}': {duplicate_summary}"
    )
