from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import pandas as pd

from hoa_report.qa import normalize_loan_id

_SHEET_NAME = "Redwood Additional Data"
_LOAN_ID_COLUMN = "Loan ID"
_MONTHLY_HOA_COLUMN = "Monthly HOA Payment Amount"
_REQUIRED_COLUMNS: tuple[str, str] = (_LOAN_ID_COLUMN, _MONTHLY_HOA_COLUMN)
_MONEY_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and value != value:
        return True
    return False


def _parse_money(value: Any) -> float | None:
    if _is_blank(value):
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


def _require_columns(raw_df: pd.DataFrame, *, path: Path) -> None:
    missing = [column for column in _REQUIRED_COLUMNS if column not in raw_df.columns]
    if not missing:
        return

    required = ", ".join(_REQUIRED_COLUMNS)
    missing_summary = ", ".join(missing)
    found = ", ".join(str(column) for column in raw_df.columns)
    raise ValueError(
        "Consolidated Analytics file is missing required column(s): "
        f"{missing_summary}. Required: {required}. Found: {found}. File: {path}"
    )


def extract_consolidated_analytics_hoa(path: str | Path) -> pd.DataFrame:
    """Extract Consolidated Analytics HOA rows keyed by collateral_id."""
    path = Path(path)
    raw_df = pd.read_excel(path, sheet_name=_SHEET_NAME, dtype=object)
    _require_columns(raw_df, path=path)

    extracted = pd.DataFrame(
        {
            "collateral_id": raw_df[_LOAN_ID_COLUMN].map(normalize_loan_id),
            "hoa_monthly_dues_amount": raw_df[_MONTHLY_HOA_COLUMN].map(_parse_money),
        },
        dtype=object,
    )
    extracted = extracted.loc[extracted["collateral_id"].notna()].copy()

    duplicate_ids = sorted(
        extracted.loc[extracted["collateral_id"].duplicated(keep=False), "collateral_id"].unique()
    )
    if duplicate_ids:
        duplicate_summary = ", ".join(duplicate_ids)
        raise ValueError(
            "Consolidated Analytics extractor requires unique normalized collateral_id values. "
            f"Duplicates found: {duplicate_summary}"
        )

    extracted["hoa_monthly_dues_frequency"] = "MONTHLY"
    extracted["hoa_source"] = "CONSOLIDATED_ANALYTICS"
    extracted["hoa_source_file"] = path.name
    return extracted.loc[
        :,
        [
            "collateral_id",
            "hoa_monthly_dues_amount",
            "hoa_monthly_dues_frequency",
            "hoa_source",
            "hoa_source_file",
        ],
    ].reset_index(drop=True)

