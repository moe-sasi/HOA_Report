from __future__ import annotations

from pathlib import Path

import pandas as pd

_SOURCE_LOAN_ID_COLUMN = "Loan Number"
_OPTIONAL_COLUMN_MAP: tuple[tuple[str, str], ...] = (
    ("Monthly Dues", "hoa_monthly_dues_amount"),
    ("Monthly Dues Frequency", "hoa_monthly_dues_frequency"),
    ("Transfer Fee", "hoa_transfer_fee_amount"),
    ("Special Assessment", "hoa_special_assessment_amount"),
    ("Notes", "hoa_notes"),
)


def extract_example_vendor(path: str | Path) -> pd.DataFrame:
    """Example vendor extractor for synthetic fixtures and plug-in testing."""
    path = Path(path)
    raw_df = pd.read_excel(path, sheet_name=0, dtype=object)

    if _SOURCE_LOAN_ID_COLUMN not in raw_df.columns:
        raise ValueError(
            f"Example vendor file is missing required column '{_SOURCE_LOAN_ID_COLUMN}': {path}"
        )

    extracted = pd.DataFrame({"loan_id": raw_df[_SOURCE_LOAN_ID_COLUMN]}, dtype=object)
    for source_column, canonical_column in _OPTIONAL_COLUMN_MAP:
        if source_column in raw_df.columns:
            extracted[canonical_column] = raw_df[source_column]

    return extracted
