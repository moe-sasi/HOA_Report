from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Protocol

import pandas as pd

from hoa_report.qa import normalize_loan_id

LOAN_ID_COLUMN = "loan_id"
DEFAULT_TEMP_TABLE = "#tape_loan_ids"
_TEMP_TABLE_PATTERN = re.compile(r"^#[A-Za-z0-9_]+$")


class CursorLike(Protocol):
    def execute(self, statement: str, params: Sequence[object] | None = None) -> Any: ...

    def executemany(self, statement: str, params_list: Sequence[Sequence[object]]) -> Any: ...


def _require_column(df: pd.DataFrame, column: str, frame_name: str) -> None:
    if column not in df.columns:
        raise ValueError(f"{frame_name} must contain '{column}'")


def _normalize_required_ids(df: pd.DataFrame, *, loan_id_column: str, frame_name: str) -> pd.Series:
    _require_column(df, loan_id_column, frame_name)
    normalized = df[loan_id_column].map(normalize_loan_id)
    missing_count = int(normalized.isna().sum())
    if missing_count:
        raise ValueError(
            f"{frame_name} has {missing_count} blank/unparseable '{loan_id_column}' value(s)"
        )
    return normalized


def _normalize_unique_ids(df: pd.DataFrame, *, loan_id_column: str, frame_name: str) -> list[str]:
    normalized = _normalize_required_ids(df, loan_id_column=loan_id_column, frame_name=frame_name)
    deduped = dict.fromkeys(normalized.tolist())
    return list(deduped)


def _validate_temp_table_name(temp_table: str) -> None:
    if not _TEMP_TABLE_PATTERN.fullmatch(temp_table):
        raise ValueError(
            f"Invalid SQL temp table name '{temp_table}'. Expected format '#name' with alphanumeric/underscore."
        )


def load_ids_to_temp_table(
    cursor: CursorLike,
    tape_df: pd.DataFrame,
    *,
    temp_table: str = DEFAULT_TEMP_TABLE,
    loan_id_column: str = LOAN_ID_COLUMN,
) -> list[str]:
    """
    Load normalized tape loan IDs into a SQL Server temp table.

    Returns the ordered, de-duplicated loan IDs loaded to the temp table.
    """
    _validate_temp_table_name(temp_table)
    loan_ids = _normalize_unique_ids(tape_df, loan_id_column=loan_id_column, frame_name="tape_df")
    if not loan_ids:
        raise ValueError("tape_df produced no valid loan_id values to load")

    cursor.execute(f"IF OBJECT_ID('tempdb..{temp_table}') IS NOT NULL DROP TABLE {temp_table};")
    cursor.execute(f"CREATE TABLE {temp_table} ({loan_id_column} NVARCHAR(100) NOT NULL PRIMARY KEY);")
    cursor.executemany(
        f"INSERT INTO {temp_table} ({loan_id_column}) VALUES (?);",
        [(loan_id,) for loan_id in loan_ids],
    )
    return loan_ids


def validate_sql_enrichment_contract(
    enrichment_df: pd.DataFrame,
    *,
    loan_id_column: str = LOAN_ID_COLUMN,
) -> pd.DataFrame:
    """
    Validate SQL enrichment frame contract and return a normalized copy.

    Contract:
    - Must contain loan_id column.
    - loan_id values must be parseable.
    - loan_id values must be unique after normalization.
    """
    normalized_ids = _normalize_required_ids(
        enrichment_df,
        loan_id_column=loan_id_column,
        frame_name="enrichment_df",
    )

    duplicate_mask = normalized_ids.duplicated(keep=False)
    if duplicate_mask.any():
        duplicates = sorted(normalized_ids.loc[duplicate_mask].unique().tolist())
        duplicate_summary = ", ".join(duplicates)
        raise ValueError(
            "enrichment_df has duplicate normalized loan_id values: " f"{duplicate_summary}"
        )

    normalized_df = enrichment_df.copy()
    normalized_df[loan_id_column] = normalized_ids
    return normalized_df.reset_index(drop=True)


def merge_sql_enrichment_onto_tape(
    tape_df: pd.DataFrame,
    enrichment_df: pd.DataFrame,
    *,
    loan_id_column: str = LOAN_ID_COLUMN,
) -> pd.DataFrame:
    """
    Left-merge SQL enrichment onto tape using normalized loan_id values.
    """
    normalized_tape_ids = _normalize_required_ids(
        tape_df,
        loan_id_column=loan_id_column,
        frame_name="tape_df",
    )
    normalized_tape = tape_df.copy()
    normalized_tape[loan_id_column] = normalized_tape_ids

    normalized_enrichment = validate_sql_enrichment_contract(
        enrichment_df,
        loan_id_column=loan_id_column,
    )

    collisions = sorted(
        (set(normalized_tape.columns) & set(normalized_enrichment.columns)) - {loan_id_column}
    )
    if collisions:
        collision_summary = ", ".join(collisions)
        raise ValueError(
            "SQL enrichment columns collide with tape columns: "
            f"{collision_summary}. Rename SQL columns before merging."
        )

    merged = normalized_tape.merge(normalized_enrichment, on=loan_id_column, how="left")
    return merged.reset_index(drop=True)
