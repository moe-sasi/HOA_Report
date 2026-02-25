from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import pandas as pd

from hoa_report.models import enforce_hoa_extractor_columns
from hoa_report.qa import assert_unique_vendor_ids, normalize_loan_id

_NORMALIZE_HEADER_RE = re.compile(r"[^a-z0-9]+")
_MONEY_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")

_REQUIRED_HEADERS: dict[str, tuple[str, ...]] = {
    "loan_id": (
        "loan_number",
        "loan_id",
        "loan_num",
    ),
    "hoa_monthly_dues_amount": (
        "monthly_hoa_dues",
        "monthly_dues",
        "hoa_monthly_dues",
        "hoa_dues",
    ),
}


def _normalize_header_name(value: object) -> str:
    text = _NORMALIZE_HEADER_RE.sub("_", str(value).strip().lower())
    return text.strip("_")


def _resolve_required_column(
    raw_df: pd.DataFrame,
    *,
    required_key: str,
    path: Path,
) -> object:
    aliases = _REQUIRED_HEADERS[required_key]
    normalized_columns = {_normalize_header_name(column): column for column in raw_df.columns}
    for alias in aliases:
        resolved = normalized_columns.get(alias)
        if resolved is not None:
            return resolved

    expected = ", ".join(sorted(aliases))
    found = ", ".join(sorted(normalized_columns))
    raise ValueError(
        f"DD HOA file is missing required '{required_key}' column ({expected}): {path}. "
        f"Found normalized headers: {found}"
    )


def _parse_money(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()

    text = text.replace("$", "").replace(",", "").replace(" ", "")
    if not text or not _MONEY_RE.match(text):
        return None

    parsed = float(text)
    if negative:
        return -abs(parsed)
    return parsed


def _first_non_null(values: pd.Series) -> float | None:
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.iloc[0])


def extract_dd_hoa(path: str | Path) -> pd.DataFrame:
    """Extract DD HOA rows into canonical HOA output columns."""
    path = Path(path)
    raw_df = pd.read_excel(path, sheet_name=0, dtype=object)

    loan_id_column = _resolve_required_column(raw_df, required_key="loan_id", path=path)
    monthly_dues_column = _resolve_required_column(
        raw_df,
        required_key="hoa_monthly_dues_amount",
        path=path,
    )

    extracted = pd.DataFrame(
        {
            "loan_id": raw_df[loan_id_column].map(normalize_loan_id),
            "hoa_monthly_dues_amount": raw_df[monthly_dues_column].map(_parse_money),
        },
        dtype=object,
    )
    extracted = extracted.loc[extracted["loan_id"].notna()].copy()

    # Vendor files can repeat rows for a loan; keep one canonical row per loan ID.
    extracted = extracted.groupby("loan_id", as_index=False, sort=False).agg(
        {"hoa_monthly_dues_amount": _first_non_null}
    )

    canonical_df = enforce_hoa_extractor_columns(extracted)
    canonical_df["hoa_monthly_dues_frequency"] = "MONTHLY"
    canonical_df["hoa_source"] = "DD Firm"
    canonical_df["hoa_source_file"] = os.path.basename(path)

    assert_unique_vendor_ids(canonical_df, "loan_id")
    return canonical_df.reset_index(drop=True)
