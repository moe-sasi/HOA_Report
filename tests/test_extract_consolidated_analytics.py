from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hoa_report.extractors import extract_consolidated_analytics_hoa

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_fixture(filename: str, df: pd.DataFrame) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    fixture_path = _TEST_TMP_DIR / filename
    with pd.ExcelWriter(fixture_path) as writer:
        df.to_excel(writer, index=False, sheet_name="Redwood Additional Data")
    return fixture_path


def test_extract_consolidated_analytics_hoa_parses_amounts_and_columns() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan ID": [" COL-1 ", "abc.0", "L-3", None],
            "Monthly HOA Payment Amount": ["$125.00", "0", None, "300"],
            "Ignored": ["x", "x", "x", "x"],
        }
    )
    fixture_path = _write_fixture("consolidated.synthetic.xlsx", raw_df)

    extracted = extract_consolidated_analytics_hoa(fixture_path)

    assert extracted.columns.tolist() == [
        "collateral_id",
        "hoa_monthly_dues_amount",
        "hoa_monthly_dues_frequency",
        "hoa_source",
        "hoa_source_file",
    ]
    assert extracted["collateral_id"].tolist() == ["COL1", "ABC", "L3"]
    dues = extracted["hoa_monthly_dues_amount"].tolist()
    assert dues[0] == 125.0
    assert dues[1] == 0.0
    assert pd.isna(dues[2])
    assert extracted["hoa_monthly_dues_frequency"].tolist() == ["MONTHLY", "MONTHLY", "MONTHLY"]
    assert extracted["hoa_source"].tolist() == [
        "CONSOLIDATED_ANALYTICS",
        "CONSOLIDATED_ANALYTICS",
        "CONSOLIDATED_ANALYTICS",
    ]
    assert extracted["hoa_source_file"].tolist() == [fixture_path.name, fixture_path.name, fixture_path.name]


def test_extract_consolidated_analytics_hoa_raises_when_columns_missing() -> None:
    raw_df = pd.DataFrame({"Loan ID": ["COL-1"]})
    fixture_path = _write_fixture("consolidated.missing_columns.synthetic.xlsx", raw_df)

    with pytest.raises(ValueError, match="missing required column"):
        extract_consolidated_analytics_hoa(fixture_path)


def test_extract_consolidated_analytics_hoa_raises_on_duplicate_collateral_id() -> None:
    raw_df = pd.DataFrame(
        {
            "Loan ID": ["AB-22", "ab22"],
            "Monthly HOA Payment Amount": ["100", "200"],
        }
    )
    fixture_path = _write_fixture("consolidated.duplicates.synthetic.xlsx", raw_df)

    with pytest.raises(ValueError, match="Duplicates found: AB22"):
        extract_consolidated_analytics_hoa(fixture_path)

