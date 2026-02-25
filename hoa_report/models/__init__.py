"""Internal data model contracts for canonical pipeline structures."""

from hoa_report.models.canonical import (
    HOA_EXTRACTOR_COLUMNS,
    HOA_WIDE_CANONICAL_COLUMNS,
    LOAN_MASTER_CANONICAL_COLUMNS,
    LOAN_MASTER_REQUIRED_COLUMNS,
    LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS,
    build_hoa_extractor_df,
    empty_loan_master_df,
    enforce_hoa_extractor_columns,
)

__all__ = [
    "HOA_EXTRACTOR_COLUMNS",
    "HOA_WIDE_CANONICAL_COLUMNS",
    "LOAN_MASTER_CANONICAL_COLUMNS",
    "LOAN_MASTER_REQUIRED_COLUMNS",
    "LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS",
    "build_hoa_extractor_df",
    "empty_loan_master_df",
    "enforce_hoa_extractor_columns",
]
