from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import re
from typing import Any

import pandas as pd

from hoa_report.models import HOA_WIDE_CANONICAL_COLUMNS, enforce_hoa_extractor_columns
from hoa_report.qa import normalize_loan_id

_SOURCE_COLUMNS: tuple[str, str] = ("hoa_source", "hoa_source_file")
_HOA_VALUE_COLUMNS: tuple[str, ...] = tuple(
    column for column in HOA_WIDE_CANONICAL_COLUMNS if column not in _SOURCE_COLUMNS
)
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class _PreparedSource:
    original_index: int
    source_name: str
    source_file: str | None
    canonical_df: pd.DataFrame


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and value != value:
        return True
    return False


def _normalize_source_key(name: str) -> str:
    normalized = _NON_ALNUM.sub("", name.strip().lower())
    return normalized


def _first_non_blank(values: pd.Series) -> str | None:
    for value in values:
        if _is_blank(value):
            continue
        return str(value).strip()
    return None


def _prepare_source(df: pd.DataFrame, original_index: int) -> _PreparedSource:
    canonical_df = enforce_hoa_extractor_columns(df).copy()
    canonical_df["loan_id"] = canonical_df["loan_id"].map(normalize_loan_id)
    canonical_df = canonical_df.loc[canonical_df["loan_id"].notna()].copy()

    if canonical_df["loan_id"].duplicated().any():
        duplicates = sorted(canonical_df.loc[canonical_df["loan_id"].duplicated(), "loan_id"].unique())
        duplicate_summary = ", ".join(duplicates)
        raise ValueError(
            "merge_hoa_sources requires each vendor source to contain unique normalized "
            f"loan_id values. Duplicates found: {duplicate_summary}"
        )

    source_name = _first_non_blank(canonical_df["hoa_source"]) or f"source_{original_index + 1}"
    source_file = _first_non_blank(canonical_df["hoa_source_file"])
    return _PreparedSource(
        original_index=original_index,
        source_name=source_name,
        source_file=source_file,
        canonical_df=canonical_df.reset_index(drop=True),
    )


def _resolve_priority_order(
    prepared_sources: Sequence[_PreparedSource],
    priority: Sequence[str],
) -> list[_PreparedSource]:
    priority_rank: dict[str, int] = {}
    for index, name in enumerate(priority):
        normalized = _normalize_source_key(name)
        if normalized and normalized not in priority_rank:
            priority_rank[normalized] = index

    fallback_rank = len(priority_rank)
    return sorted(
        prepared_sources,
        key=lambda source: (
            priority_rank.get(_normalize_source_key(source.source_name), fallback_rank),
            source.original_index,
        ),
    )


def _normalize_discrepancy_value(value: object) -> object | None:
    if _is_blank(value):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def _values_disagree(values: Sequence[object]) -> bool:
    seen: list[object] = []
    for raw_value in values:
        value = _normalize_discrepancy_value(raw_value)
        if value is None:
            continue
        if all(value != existing for existing in seen):
            seen.append(value)
            if len(seen) > 1:
                return True
    return False


def merge_hoa_sources(
    loan_master_df: pd.DataFrame,
    hoa_sources: list[pd.DataFrame],
    priority: list[str],
) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    """Merge vendor HOA enrichment onto tape rows with tape population as the driver."""
    if "loan_id" not in loan_master_df.columns:
        raise ValueError("merge_hoa_sources requires loan_master_df to contain 'loan_id'")

    merged_df = loan_master_df.copy()
    merged_df["loan_id"] = merged_df["loan_id"].map(normalize_loan_id)
    invalid_tape_ids = int(merged_df["loan_id"].isna().sum())
    if invalid_tape_ids:
        raise ValueError(
            "merge_hoa_sources found blank/unparseable loan_id values in loan_master_df "
            f"({invalid_tape_ids} rows)."
        )

    for column in HOA_WIDE_CANONICAL_COLUMNS:
        if column not in merged_df.columns:
            merged_df[column] = None

    base_values_by_column = {column: merged_df[column].copy() for column in _HOA_VALUE_COLUMNS}
    base_source_series = merged_df["hoa_source"].copy()
    base_source_file_series = merged_df["hoa_source_file"].copy()

    prepared_sources = [_prepare_source(df, idx) for idx, df in enumerate(hoa_sources)]
    ordered_sources = _resolve_priority_order(prepared_sources, priority)

    work_df = merged_df.loc[:, ["loan_id"]].copy()
    source_prefixes: list[tuple[str, _PreparedSource]] = []
    join_columns = ["loan_id", *_HOA_VALUE_COLUMNS, *_SOURCE_COLUMNS]
    for rank, source in enumerate(ordered_sources):
        prefix = f"__src_{rank}__"
        source_prefixes.append((prefix, source))
        renamed = source.canonical_df.loc[:, join_columns].rename(
            columns={column: f"{prefix}{column}" for column in join_columns if column != "loan_id"}
        )
        work_df = work_df.merge(renamed, on="loan_id", how="left")

    selected_source_rank_by_column: dict[str, pd.Series] = {}
    for column in _HOA_VALUE_COLUMNS:
        selected_values = pd.Series([None] * len(merged_df), dtype=object)
        selected_rank = pd.Series([-1] * len(merged_df), dtype="int64")

        for rank, (prefix, _source) in enumerate(source_prefixes):
            source_values = work_df[f"{prefix}{column}"]
            fill_mask = selected_values.map(_is_blank) & ~source_values.map(_is_blank)
            selected_values.loc[fill_mask] = source_values.loc[fill_mask]
            selected_rank.loc[fill_mask] = rank

        base_values = base_values_by_column[column]
        fallback_mask = selected_values.map(_is_blank) & ~base_values.map(_is_blank)
        selected_values.loc[fallback_mask] = base_values.loc[fallback_mask]
        selected_rank.loc[fallback_mask] = -2

        merged_df[column] = selected_values
        selected_source_rank_by_column[column] = selected_rank

    source_used = pd.Series([None] * len(merged_df), dtype=object)
    source_file_used = pd.Series([None] * len(merged_df), dtype=object)
    for rank, (prefix, source) in enumerate(source_prefixes):
        contribution_mask = pd.Series(False, index=merged_df.index)
        for column in _HOA_VALUE_COLUMNS:
            contribution_mask |= selected_source_rank_by_column[column].eq(rank)

        resolved_source = work_df[f"{prefix}hoa_source"].copy()
        source_blank_mask = resolved_source.map(_is_blank)
        resolved_source.loc[source_blank_mask] = source.source_name

        resolved_file = work_df[f"{prefix}hoa_source_file"].copy()
        file_blank_mask = resolved_file.map(_is_blank) & (source.source_file is not None)
        resolved_file.loc[file_blank_mask] = source.source_file

        fill_source_mask = source_used.map(_is_blank) & contribution_mask
        fill_file_mask = source_file_used.map(_is_blank) & contribution_mask
        source_used.loc[fill_source_mask] = resolved_source.loc[fill_source_mask]
        source_file_used.loc[fill_file_mask] = resolved_file.loc[fill_file_mask]

    source_fallback_mask = source_used.map(_is_blank)
    source_file_fallback_mask = source_file_used.map(_is_blank)
    source_used.loc[source_fallback_mask] = base_source_series.loc[source_fallback_mask]
    source_file_used.loc[source_file_fallback_mask] = base_source_file_series.loc[
        source_file_fallback_mask
    ]

    merged_df["hoa_source_used"] = source_used
    merged_df["hoa_source_file_used"] = source_file_used

    discrepancy_flag = pd.Series(False, index=merged_df.index)
    for column in _HOA_VALUE_COLUMNS:
        comparison_series = [base_values_by_column[column]]
        for prefix, _source in source_prefixes:
            comparison_series.append(work_df[f"{prefix}{column}"])
        if len(comparison_series) < 2:
            continue

        column_disagreement = [
            _values_disagree(values)
            for values in zip(
                *(series.tolist() for series in comparison_series),
                strict=False,
            )
        ]
        discrepancy_flag |= pd.Series(column_disagreement, index=merged_df.index)

    merged_df["hoa_discrepancy_flag"] = discrepancy_flag

    tape_loan_ids = set(merged_df["loan_id"].dropna().tolist())
    vendor_exceptions: dict[str, dict[str, object]] = {}
    seen_vendor_names: dict[str, int] = {}
    for source in ordered_sources:
        vendor_loan_ids = set(source.canonical_df["loan_id"].dropna().tolist())
        missing = sorted(tape_loan_ids - vendor_loan_ids)
        extra = sorted(vendor_loan_ids - tape_loan_ids)

        vendor_key = source.source_name
        existing_count = seen_vendor_names.get(vendor_key, 0)
        if existing_count:
            vendor_key = f"{vendor_key}_{existing_count + 1}"
        seen_vendor_names[source.source_name] = existing_count + 1

        vendor_exceptions[vendor_key] = {
            "missing_in_vendor": missing,
            "extra_in_vendor": extra,
            "missing_in_vendor_count": len(missing),
            "extra_in_vendor_count": len(extra),
        }

    return merged_df.reset_index(drop=True), vendor_exceptions
