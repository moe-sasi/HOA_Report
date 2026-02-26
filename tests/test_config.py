from __future__ import annotations

import json
from pathlib import Path

import pytest

from hoa_report.config import load_config

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_config(filename: str, payload: dict[str, object]) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    config_path = _TEST_TMP_DIR / filename
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def test_load_config_reads_legacy_vendor_settings_from_config() -> None:
    config_path = _write_config(
        "config.vendor_type.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendor_paths": ["tests/fixtures/vendor_a.synthetic.xlsx"],
            "vendor_type": "example_vendor",
        },
    )

    config = load_config(config_path)
    assert len(config.vendors) == 1
    assert config.vendors[0].name == "example_vendor"
    assert config.vendors[0].type == "example_vendor"
    assert config.vendors[0].match_key == "loan_id"
    assert config.vendor_priority == ["example_vendor"]


def test_load_config_defaults_legacy_vendor_type_when_missing() -> None:
    config_path = _write_config(
        "config.default_vendor_type.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendor_paths": ["tests/fixtures/vendor_a.synthetic.xlsx"],
        },
    )

    config = load_config(config_path)
    assert len(config.vendors) == 1
    assert config.vendors[0].name == "example_vendor"
    assert config.vendors[0].type == "example_vendor"
    assert config.vendor_priority == ["example_vendor"]


def test_load_config_reads_multi_vendor_settings_and_priority() -> None:
    config_path = _write_config(
        "config.multi_vendor.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendors": [
                {
                    "name": "clayton",
                    "type": "clayton",
                    "path": "tests/fixtures/vendor_a.synthetic.xlsx",
                    "match_key": "loan_id",
                },
                {
                    "name": "consolidated_analytics",
                    "type": "consolidated_analytics",
                    "path": "tests/fixtures/vendor_b.synthetic.xlsx",
                    "match_key": "collateral_id",
                },
            ],
            "vendor_priority": ["clayton", "consolidated_analytics"],
        },
    )

    config = load_config(config_path)

    assert [vendor.name for vendor in config.vendors] == ["clayton", "consolidated_analytics"]
    assert [vendor.type for vendor in config.vendors] == ["clayton", "consolidated_analytics"]
    assert [vendor.match_key for vendor in config.vendors] == ["loan_id", "collateral_id"]
    assert config.vendor_priority == ["clayton", "consolidated_analytics"]


def test_load_config_requires_sql_connection_string_when_run_sql_true() -> None:
    config_path = _write_config(
        "config.sql.required.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "run_sql": True,
            "sql": {"query_path": "sql/hoa_enrich.sql"},
        },
    )

    with pytest.raises(ValueError, match="'connection_string' must be a non-empty string"):
        load_config(config_path)


def test_load_config_reads_sql_connection_string_and_default_query_path() -> None:
    config_path = _write_config(
        "config.sql.default_query_path.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "run_sql": True,
            "sql": {
                "connection_string": (
                    "mssql+pyodbc://@RTSQLGEN01/LOANDATA?"
                    "driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
                )
            },
        },
    )

    config = load_config(config_path)
    assert config.sql is not None
    assert config.sql.connection_string.startswith("mssql+pyodbc://@RTSQLGEN01/LOANDATA")
    assert config.sql.query_path == Path("sql/hoa_enrich.sql")


def test_load_config_reads_sql_query_path_override() -> None:
    config_path = _write_config(
        "config.sql.query_path_override.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "run_sql": True,
            "sql": {
                "connection_string": "mssql+pyodbc://@RTSQLGEN01/LOANDATA",
                "query_path": "sql/custom.sql",
            },
        },
    )

    config = load_config(config_path)
    assert config.sql is not None
    assert config.sql.query_path == Path("sql/custom.sql")
