from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from hoa_report.models import build_hoa_extractor_df
from hoa_report.qa import normalize_loan_id

_LOAN_NUMBER_HEADER = "Loan Number"
_LOAN_NUMBER_FALLBACK_INDEX = 6  # column G (1-based)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return bool(pd.isna(value))


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
    }
    return canonical_hoa_df, tape_qa
