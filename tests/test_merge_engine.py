from __future__ import annotations

import pandas as pd

from hoa_report.engine import merge_hoa_sources


def test_merge_hoa_sources_keeps_tape_population_as_driver_and_reports_missing_extra() -> None:
    loan_master_df = pd.DataFrame(
        {
            "loan_id": ["L-1", "L-2", "L-3", "L-4", "L-5"],
            "city": ["A", "B", "C", "D", "E"],
        }
    )
    vendor_df = pd.DataFrame(
        {
            "loan_id": ["L-1", "L-2"],
            "hoa_monthly_dues_amount": [100.0, 200.0],
            "hoa_source": ["vendor_a", "vendor_a"],
            "hoa_source_file": ["vendor_a.xlsx", "vendor_a.xlsx"],
        }
    )

    merged_df, exceptions = merge_hoa_sources(
        loan_master_df=loan_master_df,
        hoa_sources=[vendor_df],
        priority=["vendor_a"],
    )

    assert len(merged_df) == 5
    assert merged_df["loan_id"].tolist() == ["L1", "L2", "L3", "L4", "L5"]
    assert merged_df["hoa_monthly_dues_amount"].tolist() == [100.0, 200.0, None, None, None]

    assert exceptions["vendor_a"]["missing_in_vendor_count"] == 3
    assert exceptions["vendor_a"]["extra_in_vendor_count"] == 0
    assert exceptions["vendor_a"]["missing_in_vendor"] == ["L3", "L4", "L5"]
    assert exceptions["vendor_a"]["extra_in_vendor"] == []


def test_merge_hoa_sources_applies_priority_and_flags_discrepancy() -> None:
    loan_master_df = pd.DataFrame(
        {
            "loan_id": ["LN-1", "LN-2"],
            "hoa_source": ["semt_tape", "semt_tape"],
            "hoa_source_file": ["tape.xlsx", "tape.xlsx"],
        }
    )
    high_priority_df = pd.DataFrame(
        {
            "loan_id": ["LN-1", "LN-2"],
            "hoa_monthly_dues_amount": [125.0, None],
            "hoa_source": ["high_vendor", "high_vendor"],
            "hoa_source_file": ["high.xlsx", "high.xlsx"],
        }
    )
    low_priority_df = pd.DataFrame(
        {
            "loan_id": ["LN-1", "LN-2"],
            "hoa_monthly_dues_amount": [150.0, 225.0],
            "hoa_source": ["low_vendor", "low_vendor"],
            "hoa_source_file": ["low.xlsx", "low.xlsx"],
        }
    )

    merged_df, _exceptions = merge_hoa_sources(
        loan_master_df=loan_master_df,
        hoa_sources=[low_priority_df, high_priority_df],
        priority=["high_vendor", "low_vendor"],
    )

    assert merged_df["loan_id"].tolist() == ["LN1", "LN2"]
    assert merged_df["hoa_monthly_dues_amount"].tolist() == [125.0, 225.0]
    assert merged_df["hoa_source_used"].tolist() == ["high_vendor", "low_vendor"]
    assert merged_df["hoa_source_file_used"].tolist() == ["high.xlsx", "low.xlsx"]
    assert merged_df["hoa_discrepancy_flag"].tolist() == [True, False]
