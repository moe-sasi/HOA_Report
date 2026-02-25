from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SqlConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class InputConfig:
    tape_path: Path
    template_path: Path
    vendor_paths: list[Path] = field(default_factory=list)
    output_path: Path = Path("data/output.xlsx")
    run_sql: bool = False
    sql: SqlConfig | None = None


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{key}' must be a non-empty string")
    return value


def load_config(path: Path) -> InputConfig:
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    tape_path = Path(_require_str(raw, "tape_path"))
    template_path = Path(_require_str(raw, "template_path"))

    vendor_raw = raw.get("vendor_paths", [])
    if not isinstance(vendor_raw, list) or not all(isinstance(item, str) for item in vendor_raw):
        raise ValueError("'vendor_paths' must be a list of strings")
    vendor_paths = [Path(item) for item in vendor_raw]

    output_path = Path(raw.get("output_path", "data/output.xlsx"))
    run_sql = bool(raw.get("run_sql", False))

    sql_config = None
    if run_sql:
        sql_raw = raw.get("sql")
        if not isinstance(sql_raw, dict):
            raise ValueError("'sql' settings are required when 'run_sql' is true")
        sql_config = SqlConfig(
            host=_require_str(sql_raw, "host"),
            port=int(sql_raw.get("port", 5432)),
            database=_require_str(sql_raw, "database"),
            user=_require_str(sql_raw, "user"),
            password=_require_str(sql_raw, "password"),
        )

    return InputConfig(
        tape_path=tape_path,
        template_path=template_path,
        vendor_paths=vendor_paths,
        output_path=output_path,
        run_sql=run_sql,
        sql=sql_config,
    )


def validate_paths(config: InputConfig) -> None:
    required_files = [config.tape_path, config.template_path, *config.vendor_paths]
    missing = [str(file_path) for file_path in required_files if not file_path.exists()]
    if missing:
        raise FileNotFoundError(
            "The following configured input path(s) do not exist: " + ", ".join(missing)
        )
