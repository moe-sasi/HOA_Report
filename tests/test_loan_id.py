from __future__ import annotations

import pytest

from hoa_report.qa import assert_unique_vendor_ids, find_duplicate_ids, normalize_loan_id


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (12345.0, "12345"),
        ("  ab-12.0  ", "AB12"),
        ("a b_c", "ABC"),
        ("***", None),
        (None, None),
        (float("nan"), None),
    ],
)
def test_normalize_loan_id(raw: object, expected: str | None) -> None:
    assert normalize_loan_id(raw) == expected


def test_find_duplicate_ids_returns_duplicate_rows_with_normalized_value() -> None:
    rows = [
        {"loan_id": " abc-1.0 ", "source": "tape"},
        {"loan_id": "ABC1", "source": "vendor_a"},
        {"loan_id": "xyz2", "source": "vendor_b"},
        {"loan_id": "xyz-2", "source": "vendor_c"},
        {"loan_id": "unique", "source": "vendor_d"},
    ]

    duplicates = find_duplicate_ids(rows, "loan_id")

    assert len(duplicates) == 4
    assert {row["normalized_loan_id"] for row in duplicates} == {"ABC1", "XYZ2"}


def test_assert_unique_vendor_ids_raises_clear_exception() -> None:
    vendor_rows = [{"loan_id": " 1001.0"}, {"loan_id": "1001"}, {"loan_id": "AB-22"}]

    with pytest.raises(ValueError, match=r"Duplicate vendor loan IDs detected") as exc:
        assert_unique_vendor_ids(vendor_rows, "loan_id")

    assert "1001 (2 rows)" in str(exc.value)


def test_assert_unique_vendor_ids_allows_unique_ids() -> None:
    vendor_rows = [{"loan_id": "1001"}, {"loan_id": "1002"}, {"loan_id": "AB-22"}]

    assert_unique_vendor_ids(vendor_rows, "loan_id")
