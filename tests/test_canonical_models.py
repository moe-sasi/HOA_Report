from __future__ import annotations

from hoa_report.models import (
    HOA_EXTRACTOR_COLUMNS,
    HOA_WIDE_CANONICAL_COLUMNS,
    LOAN_MASTER_CANONICAL_COLUMNS,
    LOAN_MASTER_REQUIRED_COLUMNS,
    LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS,
    build_hoa_extractor_df,
    empty_loan_master_df,
)


def test_loan_master_canonical_columns_include_required_and_optional_placeholders() -> None:
    assert LOAN_MASTER_REQUIRED_COLUMNS == (
        "loan_id",
        "rwtloanno",
        "city",
        "state",
        "originator",
        "dd_firm",
        "dd_review_type",
    )
    assert {"bulk_id", "mers_number"}.issubset(LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS)
    assert LOAN_MASTER_CANONICAL_COLUMNS == (
        *LOAN_MASTER_REQUIRED_COLUMNS,
        *LOAN_MASTER_TEMPLATE_OPTIONAL_COLUMNS,
    )


def test_hoa_wide_mvp_columns_match_contract() -> None:
    assert HOA_WIDE_CANONICAL_COLUMNS == (
        "hoa_monthly_dues_amount",
        "hoa_monthly_dues_frequency",
        "hoa_transfer_fee_amount",
        "hoa_special_assessment_amount",
        "hoa_notes",
        "hoa_source",
        "hoa_source_file",
    )
    assert HOA_EXTRACTOR_COLUMNS == ("loan_id", *HOA_WIDE_CANONICAL_COLUMNS)


def test_empty_loan_master_df_uses_canonical_schema() -> None:
    loan_master_df = empty_loan_master_df()

    assert loan_master_df.empty
    assert loan_master_df.columns.tolist() == list(LOAN_MASTER_CANONICAL_COLUMNS)


def test_build_hoa_extractor_df_returns_only_canonical_columns() -> None:
    df = build_hoa_extractor_df(
        loan_ids=["LN1", "LN2"],
        hoa_source="vendor_a",
        hoa_source_file="vendor_a.xlsx",
    )

    assert df.columns.tolist() == list(HOA_EXTRACTOR_COLUMNS)
    assert df["loan_id"].tolist() == ["LN1", "LN2"]
    assert df["hoa_source"].tolist() == ["vendor_a", "vendor_a"]
    assert df["hoa_source_file"].tolist() == ["vendor_a.xlsx", "vendor_a.xlsx"]
    for column in HOA_WIDE_CANONICAL_COLUMNS:
        if column in {"hoa_source", "hoa_source_file"}:
            continue
        assert df[column].isna().all()
