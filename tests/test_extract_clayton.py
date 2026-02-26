from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hoa_report.extractors import extract_clayton_hoa

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_clayton_fixture(filename: str, df: pd.DataFrame) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    fixture_path = _TEST_TMP_DIR / filename
    with pd.ExcelWriter(fixture_path) as writer:
        df.to_excel(writer, index=False, sheet_name="HOA")
    return fixture_path


def test_extract_clayton_hoa_parses_amounts_and_enforces_output_columns() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan Number": [" 1001.0 ", "AB-22", "L-4", None, "  "],
            "HOA Monthly Premium Amount": ["$125.00", "0", None, "$300.00", " $99.00 "],
            "Ignored": ["x", "x", "x", "x", "x"],
        }
    )
    fixture_path = _write_clayton_fixture("clayton.synthetic.xlsx", raw_df)

    extracted = extract_clayton_hoa(fixture_path)

    assert extracted.columns.tolist() == [
        "loan_id",
        "hoa_monthly_dues_amount",
        "hoa_monthly_dues_frequency",
        "hoa_source",
        "hoa_source_file",
    ]
    assert extracted["loan_id"].tolist() == ["1001", "AB22", "L4"]
    dues = extracted["hoa_monthly_dues_amount"].tolist()
    assert dues[0] == 125.0
    assert dues[1] == 0.0
    assert pd.isna(dues[2])
    assert extracted["hoa_monthly_dues_frequency"].tolist() == ["MONTHLY", "MONTHLY", "MONTHLY"]
    assert extracted["hoa_source"].tolist() == ["CLAYTON", "CLAYTON", "CLAYTON"]
    assert extracted["hoa_source_file"].tolist() == [fixture_path.name, fixture_path.name, fixture_path.name]


def test_extract_clayton_hoa_raises_when_required_columns_are_missing() -> None:
    raw_df = pd.DataFrame({"Loan Number": ["L-1"]})
    fixture_path = _write_clayton_fixture("clayton.missing_columns.synthetic.xlsx", raw_df)

    with pytest.raises(ValueError, match="missing required column"):
        extract_clayton_hoa(fixture_path)


def test_extract_clayton_hoa_raises_when_normalized_loan_ids_are_duplicate() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan Number": ["AB-22", "ab22"],
            "HOA Monthly Premium Amount": ["100", "200"],
        }
    )
    fixture_path = _write_clayton_fixture("clayton.duplicates.synthetic.xlsx", raw_df)

    with pytest.raises(ValueError, match="Duplicates found: AB22"):
        extract_clayton_hoa(fixture_path)
