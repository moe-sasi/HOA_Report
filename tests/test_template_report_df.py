from __future__ import annotations

import pandas as pd

from hoa_report.engine import build_template_report_df

EXPECTED_TEMPLATE_COLUMNS = [
    "rwtLoanNo",
    "SEMT ID",
    "Bulk ID",
    "MERS Number",
    "Seller",
    "Collateral ID",
    "Altrernate ID",
    "Primary Servicer",
    "Servicer Loan Number",
    "RWT Purchase Date",
    "Property Address",
    "Property City",
    "Property State",
    "Property Zip",
    "HOA",
    "HOA Monthly Payment",
    "Securitized Balance",
    "Securitized Next Due Date",
    "DD Firm",
    "Review Status",
]


def test_build_template_report_df_maps_canonical_fields_and_hoa_flag() -> None:
    loan_master_df_enriched = pd.DataFrame(
        {
            "rwtloanno": ["RWT-1", "RWT-2", "RWT-3"],
            "loan_id": ["LN-1", "LN-2", "LN-3"],
            "bulk_id": ["BULK-1", "BULK-2", "BULK-3"],
            "mers_number": ["MERS-1", "MERS-2", "MERS-3"],
            "originator": ["Seller A", "Seller B", "Seller C"],
            "collateral_id": ["COLL-1", "COLL-2", "COLL-3"],
            "alternate_id": ["ALT-1", "ALT-2", "ALT-3"],
            "primary_servicer": ["Servicer A", "Servicer B", "Servicer C"],
            "servicer_loan_number": ["SLN-1", "SLN-2", "SLN-3"],
            "rwt_purchase_date": ["2026-01-15", "2026-01-16", "2026-01-17"],
            "property_address": ["1 Main St", "2 Main St", "3 Main St"],
            "city": ["Austin", "Dallas", "Houston"],
            "state": ["TX", "TX", "TX"],
            "zip_code": ["11111", "22222", "33333"],
            "hoa_monthly_dues_amount": [125.0, 0.0, None],
            "securitized_balance": [100000.0, 200000.0, 300000.0],
            "securitized_next_due_date": ["2026-03-01", "2026-03-01", "2026-03-01"],
            "dd_firm": ["DD-1", "DD-2", "DD-3"],
            "dd_review_type": ["Pass", "Manual", "Pending"],
        }
    )

    report_df = build_template_report_df(loan_master_df_enriched)

    assert report_df.columns.tolist() == EXPECTED_TEMPLATE_COLUMNS
    assert report_df["rwtLoanNo"].tolist() == ["RWT-1", "RWT-2", "RWT-3"]
    assert report_df["SEMT ID"].tolist() == ["LN-1", "LN-2", "LN-3"]
    assert report_df["Seller"].tolist() == ["Seller A", "Seller B", "Seller C"]
    assert report_df["Property City"].tolist() == ["Austin", "Dallas", "Houston"]
    assert report_df["Review Status"].tolist() == ["Pass", "Manual", "Pending"]
    assert report_df["HOA Monthly Payment"].iloc[0] == 125.0
    assert report_df["HOA Monthly Payment"].iloc[1] == 0.0
    assert pd.isna(report_df["HOA Monthly Payment"].iloc[2])
    assert report_df["Securitized Balance"].tolist() == [100000.0, 200000.0, 300000.0]
    assert report_df["HOA"].tolist() == ["Y", "N", ""]


def test_build_template_report_df_supports_template_named_columns_and_money_string_flags() -> None:
    loan_master_df_enriched = pd.DataFrame(
        {
            "SEMT ID": ["S1", "S2", "S3"],
            "HOA Monthly Payment": ["$1,250.50", "0", None],
            "Review Status": ["Complete", "Manual", ""],
            "DD Firm": ["Firm A", "Firm B", "Firm C"],
            "Property Zip": ["90001", "90002", "90003"],
        }
    )

    report_df = build_template_report_df(loan_master_df_enriched)

    assert report_df.columns.tolist() == EXPECTED_TEMPLATE_COLUMNS
    assert report_df["SEMT ID"].tolist() == ["S1", "S2", "S3"]
    assert report_df["HOA Monthly Payment"].tolist() == ["$1,250.50", "0", None]
    assert report_df["HOA"].tolist() == ["Y", "N", ""]
    assert report_df["rwtLoanNo"].isna().all()


def test_build_template_report_df_prefers_loan_id_when_semt_id_column_is_blank() -> None:
    loan_master_df_enriched = pd.DataFrame(
        {
            "loan_id": ["LN1", "LN2", "LN3"],
            "SEMT ID": ["", None, "   "],
        }
    )

    report_df = build_template_report_df(loan_master_df_enriched)

    assert report_df["SEMT ID"].tolist() == ["LN1", "LN2", "LN3"]


def test_build_template_report_df_prefers_current_loan_amount_for_securitized_balance() -> None:
    loan_master_df_enriched = pd.DataFrame(
        {
            "loan_id": ["LN1", "LN2"],
            "Current Loan AMount": [111111.0, 222222.0],
            "Securitized Balance": ["", ""],
            "securitized_balance": [999999.0, 888888.0],
        }
    )

    report_df = build_template_report_df(loan_master_df_enriched)

    assert report_df["Securitized Balance"].tolist() == [111111.0, 222222.0]
