from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

LOAN_MASTER_REQUIRED_COLUMNS: tuple[str, ...] = (
    "loan_id",
    "rwtloanno",
    "city",
    "state",
    "originator",
    "dd_firm",
    "dd_review_type",
)

LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "bulk_id",
    "mers_number",
)

LOAN_MASTER_CANONICAL_COLUMNS: tuple[str, ...] = (
    *LOAN_MASTER_REQUIRED_COLUMNS,
    *LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS,
)

HOA_WIDE_CANONICAL_COLUMNS: tuple[str, ...] = (
    "hoa_monthly_dues_amount",
    "hoa_monthly_dues_frequency",
    "hoa_transfer_fee_amount",
    "hoa_special_assessment_amount",
    "hoa_notes",
    "hoa_source",
    "hoa_source_file",
)

HOA_EXTRACTOR_COLUMNS: tuple[str, ...] = ("loan_id", *HOA_WIDE_CANONICAL_COLUMNS)


def empty_loan_master_df() -> pd.DataFrame:
    """Return an empty Loan Master frame with the canonical column contract."""
    return pd.DataFrame(columns=LOAN_MASTER_CANONICAL_COLUMNS, dtype=object)


def enforce_hoa_extractor_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Project any frame onto the canonical HOA extractor output columns."""
    canonical_df = df.copy()
    for column in HOA_EXTRACTOR_COLUMNS:
        if column not in canonical_df.columns:
            canonical_df[column] = None
    return canonical_df.loc[:, HOA_EXTRACTOR_COLUMNS]


def build_hoa_extractor_df(
    loan_ids: Iterable[object],
    *,
    hoa_source: str | None = None,
    hoa_source_file: str | None = None,
) -> pd.DataFrame:
    """Build canonical HOA extractor output rows for a set of loan IDs."""
    canonical_df = pd.DataFrame({"loan_id": list(loan_ids)}, dtype=object)
    canonical_df = enforce_hoa_extractor_columns(canonical_df)

    if hoa_source is not None:
        canonical_df["hoa_source"] = hoa_source
    if hoa_source_file is not None:
        canonical_df["hoa_source_file"] = hoa_source_file

    return canonical_df
