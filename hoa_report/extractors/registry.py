from __future__ import annotations

from pathlib import Path

import pandas as pd

from hoa_report.extractors.base import VendorExtractorFn
from hoa_report.extractors.dd_hoa import extract_dd_hoa
from hoa_report.extractors.example_vendor import extract_example_vendor
from hoa_report.models import enforce_hoa_extractor_columns
from hoa_report.qa import assert_unique_vendor_ids, normalize_loan_id

_EXTRACTOR_REGISTRY: dict[str, VendorExtractorFn] = {
    "dd_hoa": extract_dd_hoa,
    "example_vendor": extract_example_vendor,
}


def _normalize_vendor_type(vendor_type: str) -> str:
    normalized = vendor_type.strip().lower()
    if not normalized:
        raise ValueError("Vendor extractor name must be a non-empty string")
    return normalized


def list_vendor_extractors() -> tuple[str, ...]:
    """Return all known vendor extractor names."""
    return tuple(sorted(_EXTRACTOR_REGISTRY))


def register_vendor_extractor(vendor_type: str, extractor: VendorExtractorFn) -> None:
    """Register or replace a vendor extractor implementation by name."""
    normalized_type = _normalize_vendor_type(vendor_type)
    _EXTRACTOR_REGISTRY[normalized_type] = extractor


def get_vendor_extractor(vendor_type: str) -> VendorExtractorFn:
    """Resolve a vendor extractor function by vendor type."""
    normalized_type = _normalize_vendor_type(vendor_type)
    extractor = _EXTRACTOR_REGISTRY.get(normalized_type)
    if extractor is None:
        available = ", ".join(list_vendor_extractors()) or "<none>"
        raise KeyError(
            f"Unknown vendor extractor '{vendor_type}'. Available extractors: {available}"
        )
    return extractor


def _enforce_vendor_output_contract(
    extracted_df: pd.DataFrame,
    *,
    vendor_type: str,
    source_path: str | Path,
) -> pd.DataFrame:
    if "loan_id" not in extracted_df.columns:
        raise ValueError(
            f"Vendor extractor '{vendor_type}' must return a DataFrame containing 'loan_id'"
        )

    canonical_df = enforce_hoa_extractor_columns(extracted_df)
    canonical_df["loan_id"] = canonical_df["loan_id"].map(normalize_loan_id)

    invalid_id_count = int(canonical_df["loan_id"].isna().sum())
    if invalid_id_count:
        raise ValueError(
            f"Vendor extractor '{vendor_type}' produced {invalid_id_count} blank/unparseable "
            "loan_id value(s) after normalization"
        )

    assert_unique_vendor_ids(canonical_df, "loan_id")
    source_mask = canonical_df["hoa_source"].map(_is_blank)
    source_file_mask = canonical_df["hoa_source_file"].map(_is_blank)
    canonical_df.loc[source_mask, "hoa_source"] = vendor_type
    canonical_df.loc[source_file_mask, "hoa_source_file"] = str(Path(source_path))
    return canonical_df.reset_index(drop=True)


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and value != value:
        return True
    return False


def extract_vendor_file(vendor_type: str, path: str | Path) -> pd.DataFrame:
    """Run a registered extractor and enforce canonical vendor output rules."""
    normalized_type = _normalize_vendor_type(vendor_type)
    extractor = get_vendor_extractor(normalized_type)
    extracted_df = extractor(path)
    if not isinstance(extracted_df, pd.DataFrame):
        raise TypeError(
            f"Vendor extractor '{normalized_type}' returned {type(extracted_df).__name__}, "
            "expected pandas.DataFrame"
        )
    return _enforce_vendor_output_contract(
        extracted_df,
        vendor_type=normalized_type,
        source_path=path,
    )
