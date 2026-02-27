from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import pandas as pd

from hoa_report.models import build_hoa_extractor_df
from hoa_report.qa import normalize_loan_id

_LOAN_NUMBER_HEADER = "Loan Number"
_LOAN_NUMBER_FALLBACK_INDEX = 6  # column G (1-based)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_DD_FIRM_ALIASES: tuple[str, ...] = (
    "DD Firm",
    "DueDiligenceVendor",
    "Due Diligence Vendor",
    "dd_firm",
    "due_diligence_vendor",
)
_REVIEW_STATUS_ALIASES: tuple[str, ...] = (
    "Review Status",
    "DD Review Type",
    "SubLoanReviewType",
    "dd_review_type",
    "review_status",
)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return bool(pd.isna(value))


def _normalize_column_key(value: object) -> str:
    return _NON_ALNUM.sub("", str(value).strip().lower())


def _build_column_lookup(columns: pd.Index) -> dict[str, object]:
    lookup: dict[str, object] = {}
    for column in columns:
        key = _normalize_column_key(column)
        if key and key not in lookup:
            lookup[key] = column
    return lookup


def _resolve_optional_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> object | None:
    column_lookup = _build_column_lookup(df.columns)
    for alias in aliases:
        key = _normalize_column_key(alias)
        if key in column_lookup:
            return column_lookup[key]
    return None


def _clean_optional_text(value: object) -> object | None:
    if _is_blank(value):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def _extract_optional_column_values(
    *,
    df: pd.DataFrame,
    aliases: tuple[str, ...],
) -> tuple[pd.Series, str | None]:
    resolved_column = _resolve_optional_column(df, aliases)
    if resolved_column is None:
        return pd.Series([None] * len(df), index=df.index, dtype=object), None
    return df[resolved_column].map(_clean_optional_text), str(resolved_column)


def _resolve_loan_number_column(df: pd.DataFrame) -> tuple[object, str]:
    if _LOAN_NUMBER_HEADER in df.columns:
        return _LOAN_NUMBER_HEADER, "header"

    if len(df.columns) <= _LOAN_NUMBER_FALLBACK_INDEX:
        raise ValueError(
            "SEMT tape is missing 'Loan Number' header and has fewer than 7 columns; "
            "cannot fallback to column G."
        )

    fallback_col = df.columns[_LOAN_NUMBER_FALLBACK_INDEX]
    fallback_values = df[fallback_col]
    non_blank_count = int((~fallback_values.map(_is_blank)).sum())
    if non_blank_count == 0:
        raise ValueError(
            "SEMT tape is missing 'Loan Number' header and fallback column G is blank; "
            "cannot infer loan numbers."
        )

    return fallback_col, "column_g_fallback"


def extract_semt_tape(tape_path: str | Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Extract SEMT tape rows using authoritative loan population rules."""
    tape_path = Path(tape_path)
    raw_df = pd.read_excel(tape_path, sheet_name=0, dtype=object)

    loan_number_column, resolution = _resolve_loan_number_column(raw_df)
    non_blank_loan_numbers = ~raw_df[loan_number_column].map(_is_blank)
    extracted_rows = raw_df.loc[non_blank_loan_numbers].copy()

    loan_ids = extracted_rows[loan_number_column].map(normalize_loan_id)
    duplicate_mask = loan_ids.notna() & loan_ids.duplicated(keep=False)

    canonical_hoa_df = build_hoa_extractor_df(
        loan_ids=loan_ids.tolist(),
        hoa_source="semt_tape",
        hoa_source_file=str(tape_path),
    )
    dd_firm_values, dd_firm_column = _extract_optional_column_values(
        df=extracted_rows,
        aliases=_DD_FIRM_ALIASES,
    )
    review_status_values, review_status_column = _extract_optional_column_values(
        df=extracted_rows,
        aliases=_REVIEW_STATUS_ALIASES,
    )
    canonical_hoa_df["dd_firm"] = dd_firm_values.to_numpy(copy=False)
    canonical_hoa_df["dd_review_type"] = review_status_values.to_numpy(copy=False)

    duplicate_ids = sorted(loan_ids.loc[duplicate_mask].dropna().unique().tolist())
    tape_qa = {
        "tape_path": str(tape_path),
        "input_row_count": int(len(raw_df)),
        "loan_row_count": int(len(canonical_hoa_df)),
        "dropped_blank_loan_number_rows": int((~non_blank_loan_numbers).sum()),
        "loan_number_column": str(loan_number_column),
        "loan_number_resolution": resolution,
        "duplicate_loan_id_count": int(duplicate_mask.sum()),
        "duplicate_loan_ids": duplicate_ids,
        "dd_firm_column": dd_firm_column,
        "review_status_column": review_status_column,
    }
    return canonical_hoa_df, tape_qa
