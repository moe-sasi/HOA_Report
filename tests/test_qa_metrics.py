from __future__ import annotations

import pandas as pd
import pytest

from hoa_report.qa import compute_qa


def test_compute_qa_with_full_inputs_returns_expected_metrics() -> None:
    tape_df = pd.DataFrame({"loan_id": ["L-1", "L-1", "L-2", "L-3"]})
    vendor_df = pd.DataFrame({"loan_id": ["L-1", "L-2", "L-2", "L-4"]})
    merged_df = pd.DataFrame(
        {
            "loan_id": ["L-1", "L-1", "L-2", "L-3"],
            "hoa_monthly_dues_amount": [100.0, None, 0.0, None],
        }
    )

    qa_df, qa_dict = compute_qa(
        tape_df=tape_df,
        vendor_df=vendor_df,
        merged_df=merged_df,
        tape_raw_rows=6,
    )

    assert qa_df.columns.tolist() == ["Metric", "Value"]
    assert qa_dict["tape_rows"] == 4
    assert qa_dict["tape_rows_raw"] == 6
    assert qa_dict["tape_rows_unique"] == 3
    assert qa_dict["tape_rows_dupes"] == 1
    assert qa_dict["vendor_rows"] == 4
    assert qa_dict["vendor_rows_unique"] == 3
    assert qa_dict["matched"] == 2
    assert qa_dict["match_rate"] == pytest.approx(2 / 3)
    assert qa_dict["missing_hoa_values_count"] == 2

    by_metric = dict(zip(qa_df["Metric"].tolist(), qa_df["Value"].tolist(), strict=True))
    assert by_metric["Tape Rows"] == 4
    assert by_metric["Tape Raw Rows"] == 6
    assert by_metric["Tape Unique Loan IDs"] == 3
    assert by_metric["Tape Duplicate Loan IDs"] == 1
    assert by_metric["Vendor Rows"] == 4
    assert by_metric["Vendor Unique Loan IDs"] == 3
    assert by_metric["Matched Loan IDs"] == 2
    assert by_metric["Match Rate"] == pytest.approx(2 / 3)
    assert by_metric["Missing HOA Values Count"] == 2


def test_compute_qa_handles_partial_inputs() -> None:
    merged_df = pd.DataFrame(
        {
            "loan_id": ["A-1", "A-2", None],
            "hoa_monthly_dues_amount": [None, "150", None],
        }
    )

    qa_df, qa_dict = compute_qa(merged_df=merged_df)

    assert qa_df.columns.tolist() == ["Metric", "Value"]
    assert qa_dict["tape_rows"] == 3
    assert qa_dict["tape_rows_raw"] == 3
    assert qa_dict["tape_rows_unique"] == 2
    assert qa_dict["tape_rows_dupes"] == 0
    assert qa_dict["vendor_rows"] == 0
    assert qa_dict["vendor_rows_unique"] == 0
    assert qa_dict["matched"] == 1
    assert qa_dict["match_rate"] == pytest.approx(0.5)
    assert qa_dict["missing_hoa_values_count"] == 2
