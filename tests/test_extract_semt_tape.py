from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hoa_report.extractors import extract_semt_tape
from hoa_report.models import HOA_EXTRACTOR_COLUMNS, HOA_WIDE_CANONICAL_COLUMNS

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_synthetic_tape(filename: str, df: pd.DataFrame) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    tape_path = _TEST_TMP_DIR / filename
    df.to_excel(tape_path, index=False)
    return tape_path


def test_extract_semt_tape_uses_exact_loan_number_header_first() -> None:
    df = pd.DataFrame(
        {
            "A": ["first", "drop-blank", "third", "fourth", "drop-none"],
            "B": [1, 2, 3, 4, 5],
            "Loan Number": [" L-1001.0 ", "   ", "AB-22", "ab22", None],
            "DD Firm": ["Firm A", "Drop Firm", "Firm B", "   ", "Drop Firm 2"],
            "Review Status": ["Pass", "Drop", "Manual", None, "Drop 2"],
            "Current Loan AMount": [101000, 999999, 202000, " ", 303000],
            "D": ["x", "x", "x", "x", "x"],
            "E": ["x", "x", "x", "x", "x"],
            "F": ["x", "x", "x", "x", "x"],
            "G": ["not", "a", "loan", "number", "column"],
        }
    )
    tape_path = _write_synthetic_tape("semt_header.synthetic.xlsx", df)

    loan_master_df, tape_qa = extract_semt_tape(tape_path)

    assert loan_master_df.columns.tolist()[: len(HOA_EXTRACTOR_COLUMNS)] == list(HOA_EXTRACTOR_COLUMNS)
    assert {"dd_firm", "dd_review_type"}.issubset(loan_master_df.columns)
    assert loan_master_df["loan_id"].tolist() == ["L1001", "AB22", "AB22"]
    assert loan_master_df["hoa_source"].tolist() == ["semt_tape", "semt_tape", "semt_tape"]
    assert loan_master_df["hoa_source_file"].tolist() == [str(tape_path), str(tape_path), str(tape_path)]
    assert loan_master_df["dd_firm"].tolist() == ["Firm A", "Firm B", None]
    assert loan_master_df["dd_review_type"].tolist() == ["Pass", "Manual", None]
    assert loan_master_df["current_loan_amount"].iloc[0] == 101000
    assert loan_master_df["current_loan_amount"].iloc[1] == 202000
    assert pd.isna(loan_master_df["current_loan_amount"].iloc[2])
    for column in HOA_WIDE_CANONICAL_COLUMNS:
        if column in {"hoa_source", "hoa_source_file"}:
            continue
        assert loan_master_df[column].isna().all()

    assert tape_qa["loan_number_resolution"] == "header"
    assert tape_qa["loan_number_column"] == "Loan Number"
    assert tape_qa["dropped_blank_loan_number_rows"] == 2
    assert tape_qa["duplicate_loan_id_count"] == 2
    assert tape_qa["duplicate_loan_ids"] == ["AB22"]
    assert tape_qa["dd_firm_column"] == "DD Firm"
    assert tape_qa["review_status_column"] == "Review Status"
    assert tape_qa["current_loan_amount_column"] == "Current Loan AMount"


def test_extract_semt_tape_falls_back_to_column_g() -> None:
    df = pd.DataFrame(
        {
            "A": ["row-1", "row-2", "row-3"],
            "B": [10, 20, 30],
            "C": ["x", "x", "x"],
            "D": ["x", "x", "x"],
            "E": ["x", "x", "x"],
            "F": ["x", "x", "x"],
            "G_value": ["LN-1", "   ", "LN-3"],
            "H": ["tail", "tail", "tail"],
        }
    )
    tape_path = _write_synthetic_tape("semt_fallback.synthetic.xlsx", df)

    loan_master_df, tape_qa = extract_semt_tape(tape_path)

    assert loan_master_df.columns.tolist()[: len(HOA_EXTRACTOR_COLUMNS)] == list(HOA_EXTRACTOR_COLUMNS)
    assert {"dd_firm", "dd_review_type"}.issubset(loan_master_df.columns)
    assert loan_master_df["loan_id"].tolist() == ["LN1", "LN3"]
    assert loan_master_df["hoa_source"].tolist() == ["semt_tape", "semt_tape"]
    assert loan_master_df["hoa_source_file"].tolist() == [str(tape_path), str(tape_path)]
    assert loan_master_df["dd_firm"].isna().all()
    assert loan_master_df["dd_review_type"].isna().all()
    assert loan_master_df["current_loan_amount"].isna().all()
    for column in HOA_WIDE_CANONICAL_COLUMNS:
        if column in {"hoa_source", "hoa_source_file"}:
            continue
        assert loan_master_df[column].isna().all()

    assert tape_qa["loan_number_resolution"] == "column_g_fallback"
    assert tape_qa["loan_number_column"] == "G_value"
    assert tape_qa["dropped_blank_loan_number_rows"] == 1
    assert tape_qa["dd_firm_column"] is None
    assert tape_qa["review_status_column"] is None
    assert tape_qa["current_loan_amount_column"] is None


def test_extract_semt_tape_maps_due_diligence_alias_headers() -> None:
    df = pd.DataFrame(
        {
            "Loan Number": ["L-1", "L-2"],
            "DueDiligenceVendor": ["Firm X", "Firm Y"],
            "SubLoanReviewType": ["Complete", "Pending"],
            "CurrentLoanAmount": [150000, 250000],
        }
    )
    tape_path = _write_synthetic_tape("semt_dd_alias.synthetic.xlsx", df)

    loan_master_df, tape_qa = extract_semt_tape(tape_path)

    assert loan_master_df["loan_id"].tolist() == ["L1", "L2"]
    assert loan_master_df["dd_firm"].tolist() == ["Firm X", "Firm Y"]
    assert loan_master_df["dd_review_type"].tolist() == ["Complete", "Pending"]
    assert loan_master_df["current_loan_amount"].tolist() == [150000, 250000]
    assert tape_qa["dd_firm_column"] == "DueDiligenceVendor"
    assert tape_qa["review_status_column"] == "SubLoanReviewType"
    assert tape_qa["current_loan_amount_column"] == "CurrentLoanAmount"


def test_extract_semt_tape_fails_when_fallback_column_g_does_not_exist() -> None:
    df = pd.DataFrame(
        {
            "A": [1],
            "B": [2],
            "C": [3],
            "D": [4],
            "E": [5],
            "F": [6],
        }
    )
    tape_path = _write_synthetic_tape("semt_missing_g.synthetic.xlsx", df)

    with pytest.raises(ValueError, match=r"fewer than 7 columns"):
        extract_semt_tape(tape_path)


def test_extract_semt_tape_fails_when_fallback_column_g_is_blank() -> None:
    df = pd.DataFrame(
        {
            "A": [1, 2],
            "B": [1, 2],
            "C": [1, 2],
            "D": [1, 2],
            "E": [1, 2],
            "F": [1, 2],
            "G_value": [" ", None],
        }
    )
    tape_path = _write_synthetic_tape("semt_blank_g.synthetic.xlsx", df)

    with pytest.raises(ValueError, match=r"fallback column G is blank"):
        extract_semt_tape(tape_path)
