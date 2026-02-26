from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qsl, unquote_plus, urlparse

import pandas as pd

from hoa_report.sql.load_ids import load_ids_to_temp_table


def _build_pyodbc_connection_string(connection_string: str) -> str:
    parsed = urlparse(connection_string)
    if parsed.scheme.lower() != "mssql+pyodbc":
        raise ValueError(
            "Only 'mssql+pyodbc' connection strings are supported without SQLAlchemy."
        )

    host = parsed.hostname
    database = parsed.path.lstrip("/")
    if not host or not database:
        raise ValueError("connection_string must include SQL Server host and database.")

    server = host if parsed.port is None else f"{host},{parsed.port}"
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=False))
    driver = unquote_plus(query_params.pop("driver", "ODBC Driver 17 for SQL Server"))

    odbc_parts: list[str] = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
    ]

    if parsed.username:
        odbc_parts.append(f"UID={unquote_plus(parsed.username)}")
    if parsed.password:
        odbc_parts.append(f"PWD={unquote_plus(parsed.password)}")

    for key, value in query_params.items():
        if not value:
            continue
        odbc_key = "Trusted_Connection" if key.lower() == "trusted_connection" else key
        odbc_parts.append(f"{odbc_key}={value}")

    return ";".join(odbc_parts)


def _run_enrichment_sql_on_connection(
    tape_df: pd.DataFrame,
    *,
    raw_connection: object,
    query_sql: str,
) -> pd.DataFrame:
    cursor = raw_connection.cursor()
    try:
        load_ids_to_temp_table(cursor, tape_df)
        raw_connection.commit()
        cursor.execute(query_sql)
        while True:
            if cursor.description is not None:
                columns = [col_desc[0] for col_desc in cursor.description]
                rows = cursor.fetchall()
                enrichment_df = pd.DataFrame.from_records(rows, columns=columns)
                break
            if not cursor.nextset():
                raise ValueError(
                    "SQL enrichment query did not return a tabular result set."
                )
    finally:
        cursor.close()
    return enrichment_df.reset_index(drop=True)


def run_sql_enrichment_query(
    tape_df: pd.DataFrame,
    *,
    connection_string: str,
    query_path: str | Path = Path("sql/hoa_enrich.sql"),
) -> pd.DataFrame:
    """
    Execute enrichment SQL using a pyodbc-backed SQLAlchemy engine.

    The query is executed in the same DB session used to load #tape_loan_ids.
    """
    query_file = Path(query_path)
    if not query_file.exists():
        raise FileNotFoundError(f"SQL enrichment query file not found: {query_file}")

    query_sql = query_file.read_text(encoding="utf-8")
    if not query_sql.strip():
        raise ValueError(f"SQL enrichment query file is empty: {query_file}")

    try:
        from sqlalchemy import create_engine
    except ImportError:
        create_engine = None

    if create_engine is not None:
        engine = create_engine(connection_string)
        raw_connection = engine.raw_connection()
        try:
            enrichment_df = _run_enrichment_sql_on_connection(
                tape_df,
                raw_connection=raw_connection,
                query_sql=query_sql,
            )
        finally:
            raw_connection.close()
            engine.dispose()
        return enrichment_df

    try:
        import pyodbc
    except ImportError as exc:  # pragma: no cover - import guard
        raise RuntimeError(
            "SQL enrichment requires 'pyodbc' or SQLAlchemy. Install: pyodbc (and optionally sqlalchemy)."
        ) from exc

    pyodbc_connection_string = _build_pyodbc_connection_string(connection_string)
    raw_connection = pyodbc.connect(pyodbc_connection_string)
    try:
        enrichment_df = _run_enrichment_sql_on_connection(
            tape_df,
            raw_connection=raw_connection,
            query_sql=query_sql,
        )
    finally:
        raw_connection.close()

    return enrichment_df
