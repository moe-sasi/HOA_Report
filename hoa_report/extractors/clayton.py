from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import pandas as pd

from hoa_report.qa import normalize_loan_id

_SHEET_NAME = "HOA"
_LOAN_NUMBER_COLUMN = "Loan Number"
_MONTHLY_PREMIUM_COLUMN = "HOA Monthly Premium Amount"
_REQUIRED_COLUMNS: tuple[str, str] = (_LOAN_NUMBER_COLUMN, _MONTHLY_PREMIUM_COLUMN)
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
        "Clayton HOA file is missing required column(s): "
        f"{missing_summary}. Required: {required}. Found: {found}. File: {path}"
    )


def extract_clayton_hoa(path: str | Path) -> pd.DataFrame:
    """Extract Clayton HOA rows into canonical Clayton columns."""
    path = Path(path)
    raw_df = pd.read_excel(path, sheet_name=_SHEET_NAME, dtype=object)
    _require_columns(raw_df, path=path)

    extracted = pd.DataFrame(
        {
            "loan_id": raw_df[_LOAN_NUMBER_COLUMN].map(normalize_loan_id),
            "hoa_monthly_dues_amount": raw_df[_MONTHLY_PREMIUM_COLUMN].map(_parse_money),
        },
        dtype=object,
    )
    extracted = extracted.loc[extracted["loan_id"].notna()].copy()

    duplicate_ids = sorted(extracted.loc[extracted["loan_id"].duplicated(keep=False), "loan_id"].unique())
    if duplicate_ids:
        duplicate_summary = ", ".join(duplicate_ids)
        raise ValueError(
            "Clayton HOA extractor requires unique normalized loan_id values. "
            f"Duplicates found: {duplicate_summary}"
        )

    extracted["hoa_monthly_dues_frequency"] = "MONTHLY"
    extracted["hoa_source"] = "CLAYTON"
    extracted["hoa_source_file"] = path.name
    return extracted.loc[
        :,
        [
            "loan_id",
            "hoa_monthly_dues_amount",
            "hoa_monthly_dues_frequency",
            "hoa_source",
            "hoa_source_file",
        ],
    ].reset_index(drop=True)
