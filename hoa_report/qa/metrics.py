from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from hoa_report.qa.loan_id import normalize_loan_id


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and value != value:
        return True
    return False


def _safe_frame_rows(df: pd.DataFrame | None) -> int:
    return 0 if df is None else int(len(df))


def _normalized_ids(df: pd.DataFrame | None, loan_id_column: str) -> pd.Series:
    if df is None or loan_id_column not in df.columns:
        return pd.Series(dtype=object)
    return df[loan_id_column].map(normalize_loan_id)


def _coerce_vendor_frames(
    vendor_df: pd.DataFrame | Sequence[pd.DataFrame] | None,
) -> list[pd.DataFrame]:
    if vendor_df is None:
        return []
    if isinstance(vendor_df, pd.DataFrame):
        return [vendor_df]
    if isinstance(vendor_df, Sequence):
        return [frame for frame in vendor_df if isinstance(frame, pd.DataFrame)]
    return []


def _build_sheet_rows(qa_dict: dict[str, int | float]) -> list[dict[str, int | float]]:
    ordered_rows = [
        ("Tape Rows", qa_dict["tape_rows"]),
        ("Tape Raw Rows", qa_dict["tape_rows_raw"]),
        ("Tape Unique Loan IDs", qa_dict["tape_rows_unique"]),
        ("Tape Duplicate Loan IDs", qa_dict["tape_rows_dupes"]),
        ("Vendor Rows", qa_dict["vendor_rows"]),
        ("Vendor Unique Loan IDs", qa_dict["vendor_rows_unique"]),
        ("Matched Loan IDs", qa_dict["matched"]),
        ("Match Rate", qa_dict["match_rate"]),
        ("Missing HOA Values Count", qa_dict["missing_hoa_values_count"]),
    ]
    return [{"Metric": metric, "Value": value} for metric, value in ordered_rows]


def compute_qa(
    *,
    tape_df: pd.DataFrame | None = None,
    vendor_df: pd.DataFrame | Sequence[pd.DataFrame] | None = None,
    merged_df: pd.DataFrame | None = None,
    tape_raw_rows: int | None = None,
    loan_id_column: str = "loan_id",
    hoa_value_column: str = "hoa_monthly_dues_amount",
) -> tuple[pd.DataFrame, dict[str, int | float]]:
    """Compute run-level QA summary metrics for tape/vendor/merge outputs."""
    effective_tape_df = tape_df if tape_df is not None else merged_df
    tape_rows = _safe_frame_rows(effective_tape_df)
    tape_ids = _normalized_ids(effective_tape_df, loan_id_column)
    tape_non_blank_ids = tape_ids.dropna()
    tape_unique = int(tape_non_blank_ids.nunique())
    tape_dupes = int(len(tape_non_blank_ids) - tape_unique)

    vendor_frames = _coerce_vendor_frames(vendor_df)
    vendor_rows = int(sum(len(frame) for frame in vendor_frames))
    vendor_id_sets: list[set[str]] = []
    for frame in vendor_frames:
        normalized = _normalized_ids(frame, loan_id_column).dropna()
        vendor_id_sets.append(set(normalized.tolist()))
    vendor_ids = set().union(*vendor_id_sets) if vendor_id_sets else set()
    vendor_unique = int(len(vendor_ids))

    tape_id_set = set(tape_non_blank_ids.tolist())

    if vendor_ids:
        matched = int(len(tape_id_set & vendor_ids))
    elif merged_df is not None and loan_id_column in merged_df.columns and hoa_value_column in merged_df.columns:
        merged_ids = merged_df[loan_id_column].map(normalize_loan_id)
        has_hoa_value = ~merged_df[hoa_value_column].map(_is_blank)
        matched = int(merged_ids.loc[has_hoa_value].dropna().nunique())
    else:
        matched = 0

    if merged_df is not None and hoa_value_column in merged_df.columns:
        missing_hoa_values_count = int(merged_df[hoa_value_column].map(_is_blank).sum())
    elif tape_unique:
        missing_hoa_values_count = max(tape_unique - matched, 0)
    else:
        missing_hoa_values_count = 0

    match_rate = float(matched / tape_unique) if tape_unique else 0.0

    qa_dict: dict[str, int | float] = {
        "tape_rows": tape_rows,
        "tape_rows_raw": int(tape_raw_rows) if tape_raw_rows is not None else tape_rows,
        "tape_rows_unique": tape_unique,
        "tape_rows_dupes": tape_dupes,
        "vendor_rows": vendor_rows,
        "vendor_rows_unique": vendor_unique,
        "matched": matched,
        "match_rate": match_rate,
        "missing_hoa_values_count": missing_hoa_values_count,
    }

    qa_df = pd.DataFrame(_build_sheet_rows(qa_dict), columns=["Metric", "Value"])
    return qa_df, qa_dict
