from __future__ import annotations

import pandas as pd
import pytest

from hoa_report.sql.load_ids import (
    load_ids_to_temp_table,
    merge_sql_enrichment_onto_tape,
    validate_sql_enrichment_contract,
)


class _MockCursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, object | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[str]]]] = []

    def execute(self, statement: str, params: object | None = None) -> None:
        self.execute_calls.append((statement, params))

    def executemany(self, statement: str, params_list: list[tuple[str]]) -> None:
        self.executemany_calls.append((statement, params_list))


def test_load_ids_to_temp_table_normalizes_and_dedupes_loan_ids() -> None:
    tape_df = pd.DataFrame({"loan_id": [" L-1001 ", "L1001", "AB-22", "AB22.0"]})
    cursor = _MockCursor()

    loaded_ids = load_ids_to_temp_table(cursor, tape_df)

    assert loaded_ids == ["L1001", "AB22"]
    assert len(cursor.execute_calls) == 2
    assert "DROP TABLE #tape_loan_ids" in cursor.execute_calls[0][0]
    assert "CREATE TABLE #tape_loan_ids" in cursor.execute_calls[1][0]

    assert len(cursor.executemany_calls) == 1
    insert_sql, params = cursor.executemany_calls[0]
    assert "INSERT INTO #tape_loan_ids (loan_id) VALUES (?)" in insert_sql
    assert params == [("L1001",), ("AB22",)]


def test_validate_sql_enrichment_contract_rejects_duplicate_normalized_ids() -> None:
    enrichment_df = pd.DataFrame({"loan_id": ["LN-1", "LN1"], "seller": ["A", "B"]})

    with pytest.raises(ValueError, match="duplicate normalized loan_id"):
        validate_sql_enrichment_contract(enrichment_df)


def test_validate_sql_enrichment_contract_requires_loan_id_column() -> None:
    enrichment_df = pd.DataFrame({"seller": ["A"]})

    with pytest.raises(ValueError, match="must contain 'loan_id'"):
        validate_sql_enrichment_contract(enrichment_df)


def test_merge_sql_enrichment_onto_tape_is_left_join_by_tape_population() -> None:
    tape_df = pd.DataFrame(
        {
            "loan_id": ["LN-1", "LN-2", "LN-3"],
            "tape_flag": ["A", "B", "C"],
        }
    )
    enrichment_df = pd.DataFrame(
        {
            "loan_id": ["LN1", "LN3"],
            "Bulk ID": ["BULK-1", "BULK-3"],
        }
    )

    merged = merge_sql_enrichment_onto_tape(tape_df, enrichment_df)

    assert merged["loan_id"].tolist() == ["LN1", "LN2", "LN3"]
    assert merged["tape_flag"].tolist() == ["A", "B", "C"]
    assert merged["Bulk ID"].iloc[0] == "BULK-1"
    assert pd.isna(merged["Bulk ID"].iloc[1])
    assert merged["Bulk ID"].iloc[2] == "BULK-3"


def test_merge_sql_enrichment_onto_tape_rejects_column_collisions() -> None:
    tape_df = pd.DataFrame({"loan_id": ["LN1"], "seller": ["Tape Seller"]})
    enrichment_df = pd.DataFrame({"loan_id": ["LN1"], "seller": ["SQL Seller"]})

    with pytest.raises(ValueError, match="columns collide"):
        merge_sql_enrichment_onto_tape(tape_df, enrichment_df)
