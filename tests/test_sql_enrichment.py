from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from hoa_report.sql.enrichment import _build_pyodbc_connection_string, _run_enrichment_sql_on_connection


def test_build_pyodbc_connection_string_from_sqlalchemy_url() -> None:
    sql_alchemy_url = (
        "mssql+pyodbc://@RTSQLGEN01/LOANDATA?"
        "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )

    odbc_conn_string = _build_pyodbc_connection_string(sql_alchemy_url)

    assert "DRIVER={ODBC Driver 17 for SQL Server}" in odbc_conn_string
    assert "server=rtsqlgen01" in odbc_conn_string.lower()
    assert "DATABASE=LOANDATA" in odbc_conn_string
    assert "Trusted_Connection=yes" in odbc_conn_string


def test_build_pyodbc_connection_string_rejects_unsupported_scheme() -> None:
    with pytest.raises(ValueError, match="Only 'mssql\\+pyodbc' connection strings"):
        _build_pyodbc_connection_string("postgresql://localhost/db")


class _MockCursor:
    def __init__(self, result_sets: list[tuple[list[str], list[tuple[Any, ...]]] | None]) -> None:
        self._result_sets = result_sets
        self._result_index = -1
        self._rows: list[tuple[Any, ...]] = []
        self.description: list[tuple[str, ...]] | None = None

    def execute(self, statement: str, params: object | None = None) -> None:
        if statement == "__query__":
            self._result_index = 0
            self._apply_current_result_set()
        else:
            self.description = None
            self._rows = []

    def executemany(self, statement: str, params_list: list[tuple[Any, ...]]) -> None:
        return None

    def nextset(self) -> bool:
        self._result_index += 1
        if self._result_index >= len(self._result_sets):
            self.description = None
            self._rows = []
            return False
        self._apply_current_result_set()
        return True

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None

    def _apply_current_result_set(self) -> None:
        current = self._result_sets[self._result_index]
        if current is None:
            self.description = None
            self._rows = []
            return
        columns, rows = current
        self.description = [(column,) for column in columns]
        self._rows = rows


class _MockConnection:
    def __init__(self, cursor: _MockCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _MockCursor:
        return self._cursor

    def commit(self) -> None:
        return None


def test_run_enrichment_sql_on_connection_handles_non_tabular_result_sets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hoa_report.sql.enrichment.load_ids_to_temp_table",
        lambda cursor, tape_df: ["L1001"],
    )
    cursor = _MockCursor(
        result_sets=[
            None,
            (["loan_id", "Seller"], [("L1001", "SQL Seller A"), ("L1002", "SQL Seller B")]),
        ]
    )
    connection = _MockConnection(cursor)

    result_df = _run_enrichment_sql_on_connection(
        pd.DataFrame({"loan_id": ["L1001"]}),
        raw_connection=connection,
        query_sql="__query__",
    )

    assert result_df.columns.tolist() == ["loan_id", "Seller"]
    assert result_df["loan_id"].tolist() == ["L1001", "L1002"]


def test_run_enrichment_sql_on_connection_errors_when_no_tabular_result_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "hoa_report.sql.enrichment.load_ids_to_temp_table",
        lambda cursor, tape_df: ["L1001"],
    )
    cursor = _MockCursor(result_sets=[None, None])
    connection = _MockConnection(cursor)

    with pytest.raises(ValueError, match="did not return a tabular result set"):
        _run_enrichment_sql_on_connection(
            pd.DataFrame({"loan_id": ["L1001"]}),
            raw_connection=connection,
            query_sql="__query__",
        )
