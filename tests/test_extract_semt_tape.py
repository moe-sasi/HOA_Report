from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hoa_report.extractors import extract_semt_tape

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
            "D": ["x", "x", "x", "x", "x"],
            "E": ["x", "x", "x", "x", "x"],
            "F": ["x", "x", "x", "x", "x"],
            "G": ["not", "a", "loan", "number", "column"],
        }
    )
    tape_path = _write_synthetic_tape("semt_header.synthetic.xlsx", df)

    loan_master_df, tape_qa = extract_semt_tape(tape_path)

    assert loan_master_df["A"].tolist() == ["first", "third", "fourth"]
    assert loan_master_df["loan_id"].tolist() == ["L1001", "AB22", "AB22"]
    assert loan_master_df["is_duplicate_loan_id"].tolist() == [False, True, True]

    assert tape_qa["loan_number_resolution"] == "header"
    assert tape_qa["loan_number_column"] == "Loan Number"
    assert tape_qa["dropped_blank_loan_number_rows"] == 2
    assert tape_qa["duplicate_loan_id_count"] == 2
    assert tape_qa["duplicate_loan_ids"] == ["AB22"]


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

    assert loan_master_df["A"].tolist() == ["row-1", "row-3"]
    assert loan_master_df["loan_id"].tolist() == ["LN1", "LN3"]
    assert loan_master_df["is_duplicate_loan_id"].tolist() == [False, False]

    assert tape_qa["loan_number_resolution"] == "column_g_fallback"
    assert tape_qa["loan_number_column"] == "G_value"
    assert tape_qa["dropped_blank_loan_number_rows"] == 1


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
