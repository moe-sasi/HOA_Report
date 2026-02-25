from __future__ import annotations

from pathlib import Path

import pandas as pd

from hoa_report.extractors import extract_dd_hoa, extract_vendor_file, list_vendor_extractors

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_synthetic_vendor_fixture(filename: str, df: pd.DataFrame) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    fixture_path = _TEST_TMP_DIR / filename
    df.to_excel(fixture_path, index=False)
    return fixture_path


def test_extract_dd_hoa_returns_unique_rows_parsed_monthly_dues_and_expected_source_fields() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan Number": [" 1001.0 ", "AB-22", "ab22", None, "  "],
            "Monthly HOA Dues ($)": ["$125.00", "1,250.50", None, "$300.00", " $99.00 "],
            "Ignored Column": ["x", "x", "x", "x", "x"],
        }
    )
    fixture_path = _write_synthetic_vendor_fixture("dd_hoa.synthetic.xlsx", raw_df)

    extracted = extract_dd_hoa(fixture_path)

    assert extracted["loan_id"].tolist() == ["1001", "AB22"]
    assert extracted["loan_id"].is_unique
    assert len(extracted) == int(extracted["loan_id"].nunique())
    assert extracted["hoa_monthly_dues_amount"].tolist() == [125.0, 1250.5]
    assert extracted["hoa_monthly_dues_frequency"].tolist() == ["MONTHLY", "MONTHLY"]
    assert extracted["hoa_source"].tolist() == ["DD Firm", "DD Firm"]
    assert extracted["hoa_source_file"].tolist() == [fixture_path.name, fixture_path.name]


def test_dd_hoa_is_registered_and_registry_preserves_extractor_metadata() -> None:
    raw_df = pd.DataFrame(
        {
            "loan id": ["L-1", "L-2"],
            "Monthly Dues": ["$100", "$200"],
        }
    )
    fixture_path = _write_synthetic_vendor_fixture("dd_hoa.registry.synthetic.xlsx", raw_df)

    assert "dd_hoa" in list_vendor_extractors()
    extracted = extract_vendor_file("dd_hoa", fixture_path)

    assert extracted["hoa_source"].tolist() == ["DD Firm", "DD Firm"]
    assert extracted["hoa_source_file"].tolist() == [fixture_path.name, fixture_path.name]
