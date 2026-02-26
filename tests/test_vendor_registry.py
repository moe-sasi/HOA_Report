from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hoa_report.extractors import extract_vendor_file, list_vendor_extractors
from hoa_report.models import HOA_EXTRACTOR_COLUMNS

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_synthetic_vendor_fixture(filename: str, df: pd.DataFrame) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    fixture_path = _TEST_TMP_DIR / filename
    df.to_excel(fixture_path, index=False)
    return fixture_path


def test_registry_contains_example_vendor_extractor() -> None:
    assert "example_vendor" in list_vendor_extractors()
    assert "clayton" in list_vendor_extractors()


def test_extract_vendor_file_normalizes_ids_enforces_uniqueness_and_canonical_schema() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan Number": [" 1001.0 ", "AB-22"],
            "Monthly Dues": [125.0, 225.0],
            "Notes": ["first", "second"],
            "Ignored": ["x", "y"],
        }
    )
    fixture_path = _write_synthetic_vendor_fixture("vendor.synthetic.xlsx", raw_df)

    extracted = extract_vendor_file("example_vendor", fixture_path)

    assert extracted.columns.tolist() == list(HOA_EXTRACTOR_COLUMNS)
    assert extracted["loan_id"].tolist() == ["1001", "AB22"]
    assert extracted["hoa_source"].tolist() == ["example_vendor", "example_vendor"]
    assert extracted["hoa_source_file"].tolist() == [str(fixture_path), str(fixture_path)]
    assert extracted["hoa_monthly_dues_amount"].tolist() == [125.0, 225.0]
    assert extracted["hoa_notes"].tolist() == ["first", "second"]
    assert extracted["hoa_transfer_fee_amount"].isna().all()


def test_extract_vendor_file_raises_on_duplicate_normalized_ids() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan Number": ["AB-22", "ab22"],
            "Monthly Dues": [100.0, 200.0],
        }
    )
    fixture_path = _write_synthetic_vendor_fixture("vendor.duplicates.synthetic.xlsx", raw_df)

    with pytest.raises(ValueError, match=r"Duplicate vendor loan IDs detected"):
        extract_vendor_file("example_vendor", fixture_path)


def test_extract_vendor_file_raises_for_unknown_vendor_type() -> None:
    raw_df = pd.DataFrame({"Loan Number": ["1001"]})
    fixture_path = _write_synthetic_vendor_fixture("vendor.unknown.synthetic.xlsx", raw_df)

    with pytest.raises(KeyError, match=r"Unknown vendor extractor"):
        extract_vendor_file("missing_vendor", fixture_path)
