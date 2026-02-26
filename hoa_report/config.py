from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SqlConfig:
    connection_string: str
    query_path: Path = Path("sql/hoa_enrich.sql")


@dataclass
class VendorInputConfig:
    name: str
    type: str
    path: Path
    match_key: str = "loan_id"


@dataclass
class InputConfig:
    tape_path: Path
    template_path: Path
    vendors: list[VendorInputConfig] = field(default_factory=list)
    vendor_priority: list[str] = field(default_factory=list)
    output_path: Path = Path("data/output.xlsx")
    run_sql: bool = False
    sql: SqlConfig | None = None


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{key}' must be a non-empty string")
    return value


def _require_str_value(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{label}' must be a non-empty string")
    return value.strip()


_SUPPORTED_MATCH_KEYS: tuple[str, ...] = ("loan_id", "collateral_id")


def _parse_vendors(raw: dict[str, Any]) -> list[VendorInputConfig]:
    vendor_entries = raw.get("vendors")
    if vendor_entries is None:
        vendor_paths_raw = raw.get("vendor_paths", [])
        if not isinstance(vendor_paths_raw, list) or not all(
            isinstance(item, str) for item in vendor_paths_raw
        ):
            raise ValueError("'vendor_paths' must be a list of strings")

        vendor_type = _require_str_value(raw.get("vendor_type", "example_vendor"), "vendor_type")
        legacy_vendors: list[VendorInputConfig] = []
        for index, path_str in enumerate(vendor_paths_raw):
            default_name = vendor_type if len(vendor_paths_raw) == 1 else f"{vendor_type}_{index + 1}"
            legacy_vendors.append(
                VendorInputConfig(
                    name=default_name,
                    type=vendor_type,
                    path=Path(path_str),
                    match_key="loan_id",
                )
            )
        return legacy_vendors

    if not isinstance(vendor_entries, list):
        raise ValueError("'vendors' must be a list of vendor config objects")

    vendors: list[VendorInputConfig] = []
    seen_names: set[str] = set()
    for index, vendor_raw in enumerate(vendor_entries):
        if not isinstance(vendor_raw, dict):
            raise ValueError(f"'vendors[{index}]' must be an object")

        name = _require_str_value(vendor_raw.get("name"), f"vendors[{index}].name")
        normalized_name = name.lower()
        if normalized_name in seen_names:
            raise ValueError(f"Vendor names must be unique (duplicate: '{name}')")
        seen_names.add(normalized_name)

        vendor_type = _require_str_value(vendor_raw.get("type"), f"vendors[{index}].type")
        path_value = _require_str_value(vendor_raw.get("path"), f"vendors[{index}].path")
        match_key = _require_str_value(
            vendor_raw.get("match_key", "loan_id"),
            f"vendors[{index}].match_key",
        ).lower()
        if match_key not in _SUPPORTED_MATCH_KEYS:
            supported = ", ".join(_SUPPORTED_MATCH_KEYS)
            raise ValueError(
                f"'vendors[{index}].match_key' must be one of: {supported}. Got: {match_key}"
            )

        vendors.append(
            VendorInputConfig(
                name=name,
                type=vendor_type,
                path=Path(path_value),
                match_key=match_key,
            )
        )
    return vendors


def _resolve_vendor_priority(
    *,
    vendors: list[VendorInputConfig],
    raw_priority: object,
) -> list[str]:
    if raw_priority is None:
        return [vendor.name for vendor in vendors]

    if not isinstance(raw_priority, list) or not all(isinstance(item, str) for item in raw_priority):
        raise ValueError("'vendor_priority' must be a list of strings")

    known_names = {vendor.name.lower(): vendor.name for vendor in vendors}
    resolved_priority: list[str] = []
    seen: set[str] = set()
    for raw_name in raw_priority:
        normalized = raw_name.strip().lower()
        if not normalized:
            raise ValueError("'vendor_priority' entries must be non-empty strings")
        canonical_name = known_names.get(normalized)
        if canonical_name is None:
            raise ValueError(
                f"'vendor_priority' contains unknown vendor name '{raw_name}'. "
                f"Known vendors: {', '.join(vendor.name for vendor in vendors) or '<none>'}"
            )
        if normalized in seen:
            raise ValueError(f"'vendor_priority' contains duplicate vendor '{raw_name}'")
        seen.add(normalized)
        resolved_priority.append(canonical_name)

    for vendor in vendors:
        if vendor.name.lower() not in seen:
            resolved_priority.append(vendor.name)
    return resolved_priority


def load_config(path: Path) -> InputConfig:
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    tape_path = Path(_require_str(raw, "tape_path"))
    template_path = Path(_require_str(raw, "template_path"))
    vendors = _parse_vendors(raw)
    vendor_priority = _resolve_vendor_priority(
        vendors=vendors,
        raw_priority=raw.get("vendor_priority"),
    )

    output_path = Path(raw.get("output_path", "data/output.xlsx"))
    run_sql = bool(raw.get("run_sql", False))

    sql_config = None
    if run_sql:
        sql_raw = raw.get("sql")
        if not isinstance(sql_raw, dict):
            raise ValueError("'sql' settings are required when 'run_sql' is true")
        query_path_raw = sql_raw.get("query_path", "sql/hoa_enrich.sql")
        if not isinstance(query_path_raw, str) or not query_path_raw.strip():
            raise ValueError("'sql.query_path' must be a non-empty string when provided")
        sql_config = SqlConfig(
            connection_string=_require_str(sql_raw, "connection_string"),
            query_path=Path(query_path_raw),
        )

    return InputConfig(
        tape_path=tape_path,
        template_path=template_path,
        vendors=vendors,
        vendor_priority=vendor_priority,
        output_path=output_path,
        run_sql=run_sql,
        sql=sql_config,
    )


def validate_paths(config: InputConfig) -> None:
    required_files = [config.tape_path, config.template_path, *(vendor.path for vendor in config.vendors)]
    if config.run_sql and config.sql is not None:
        required_files.append(config.sql.query_path)
    missing = [str(file_path) for file_path in required_files if not file_path.exists()]
    if missing:
        raise FileNotFoundError(
            "The following configured input path(s) do not exist: " + ", ".join(missing)
        )
