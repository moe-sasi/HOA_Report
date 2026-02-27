from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from hoa_report.config import InputConfig, VendorInputConfig, load_config, validate_paths
from hoa_report.engine import TEMPLATE_REPORT_COLUMNS, build_template_report_df
from hoa_report.extractors import extract_semt_tape, extract_vendor_file, get_vendor_extractor
from hoa_report.io import write_report_from_template
from hoa_report.qa import compute_qa, normalize_loan_id
from hoa_report.sql import merge_sql_enrichment_onto_tape, run_sql_enrichment_query

_QA_PRINT_ORDER: tuple[tuple[str, str], ...] = (
    ("tape_rows", "Tape Rows"),
    ("tape_rows_raw", "Tape Raw Rows"),
    ("tape_rows_unique", "Tape Unique Loan IDs"),
    ("tape_rows_dupes", "Tape Duplicate Loan IDs"),
    ("vendor_rows", "Vendor Rows"),
    ("vendor_rows_unique", "Vendor Unique Loan IDs"),
    ("matched", "Matched Loan IDs"),
    ("match_rate", "Match Rate"),
    ("missing_hoa_values_count", "Missing HOA Values Count"),
)
_HOA_VALUE_HEADER = "HOA Monthly Payment"
_HOA_FLAG_HEADER = "HOA"
_REVIEW_STATUS_HEADER = "Review Status"
_LIMITED_REVIEW_STATUS = "limited review"
_LIMITED_REVIEW_HOA_FLAG = "TBD"
_LIMITED_REVIEW_HOA_PAYMENT = "Limited Review - please refer to URAR"


@dataclass(frozen=True)
class _ProcessedVendor:
    config: VendorInputConfig
    extracted_df: pd.DataFrame
    mapped_df: pd.DataFrame
    matched_df: pd.DataFrame
    qa_summary: dict[str, int | float]
    exceptions: dict[str, object]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run HOA report pipeline")
    parser.add_argument("--config", required=True, help="Path to local JSON config")
    parser.add_argument("--tape-path", help="Override tape path from config")
    parser.add_argument("--template-path", help="Override template path from config")
    parser.add_argument(
        "--vendor-path",
        action="append",
        dest="vendor_paths",
        help="Override vendor input path(s) from config. Repeat this flag to pass multiple files.",
    )
    parser.add_argument("--out", help="Override output workbook path from config")
    parser.add_argument(
        "--vendor-type",
        help="Override vendor extractor type. With --vendor-path, applies to all override vendor files.",
    )
    return parser


def _default_vendor_type(config: InputConfig) -> str:
    if config.vendors:
        return config.vendors[0].type
    return "example_vendor"


def _build_override_vendors(override_paths: list[str], vendor_type: str) -> list[VendorInputConfig]:
    override_vendors: list[VendorInputConfig] = []
    for index, path in enumerate(override_paths):
        name = vendor_type if len(override_paths) == 1 else f"{vendor_type}_{index + 1}"
        override_vendors.append(
            VendorInputConfig(
                name=name,
                type=vendor_type,
                path=Path(path),
                match_key="loan_id",
            )
        )
    return override_vendors


def _build_effective_config(config: InputConfig, args: argparse.Namespace) -> InputConfig:
    tape_path = Path(args.tape_path) if args.tape_path else config.tape_path
    template_path = Path(args.template_path) if args.template_path else config.template_path
    output_path = Path(args.out) if args.out else config.output_path

    if args.vendor_paths:
        override_type = args.vendor_type or _default_vendor_type(config)
        vendors = _build_override_vendors(args.vendor_paths, override_type)
        vendor_priority = [vendor.name for vendor in vendors]
    else:
        vendors = [
            VendorInputConfig(
                name=vendor.name,
                type=args.vendor_type if args.vendor_type else vendor.type,
                path=vendor.path,
                match_key=vendor.match_key,
            )
            for vendor in config.vendors
        ]
        vendor_priority = list(config.vendor_priority)

    return InputConfig(
        tape_path=tape_path,
        template_path=template_path,
        vendors=vendors,
        vendor_priority=vendor_priority,
        output_path=output_path,
        run_sql=config.run_sql,
        sql=config.sql,
    )


def _format_qa_value(metric_key: str, metric_value: Any) -> str:
    if metric_key == "match_rate":
        try:
            return f"{float(metric_value):.2%}"
        except (TypeError, ValueError):
            return str(metric_value)
    return str(metric_value)


def _print_qa_summary(qa_dict: dict[str, int | float]) -> None:
    print("QA Summary")
    for metric_key, label in _QA_PRINT_ORDER:
        print(f"- {label}: {_format_qa_value(metric_key, qa_dict.get(metric_key, ''))}")


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, float) and value != value:
        return True
    return False


def _resolve_vendor_order(
    vendors: list[VendorInputConfig],
    vendor_priority: list[str],
) -> list[VendorInputConfig]:
    if not vendors:
        return []

    by_name = {vendor.name.lower(): vendor for vendor in vendors}
    ordered: list[VendorInputConfig] = []
    seen: set[str] = set()

    for raw_name in vendor_priority:
        key = raw_name.strip().lower()
        if not key:
            continue
        vendor = by_name.get(key)
        if vendor is None:
            raise ValueError(
                f"'vendor_priority' contains unknown vendor '{raw_name}'. "
                f"Known vendors: {', '.join(v.name for v in vendors)}"
            )
        if key in seen:
            continue
        seen.add(key)
        ordered.append(vendor)

    for vendor in vendors:
        key = vendor.name.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(vendor)
    return ordered


def _build_collateral_to_loan_map(
    *,
    loan_ids: pd.Series,
    report_df: pd.DataFrame,
) -> pd.DataFrame:
    if "Collateral ID" not in report_df.columns:
        raise ValueError(
            "Cannot map vendor 'collateral_id' values because report output is missing 'Collateral ID'."
        )

    map_df = pd.DataFrame(
        {
            "loan_id": loan_ids.map(normalize_loan_id),
            "collateral_id": report_df["Collateral ID"].map(normalize_loan_id),
        },
        dtype=object,
    )
    map_df = map_df.loc[map_df["loan_id"].notna() & map_df["collateral_id"].notna()].copy()

    duplicate_collateral_ids = sorted(
        map_df.loc[map_df["collateral_id"].duplicated(keep=False), "collateral_id"].unique().tolist()
    )
    if duplicate_collateral_ids:
        duplicate_summary = ", ".join(duplicate_collateral_ids)
        raise ValueError(
            "Duplicate normalized collateral_id values found in tape->collateral mapping. "
            f"Vendor mapping would be ambiguous. Duplicates: {duplicate_summary}"
        )

    return map_df.loc[:, ["loan_id", "collateral_id"]].reset_index(drop=True)


def _extract_vendor_frame(vendor: VendorInputConfig) -> pd.DataFrame:
    id_column = "loan_id" if vendor.match_key == "loan_id" else "collateral_id"
    return extract_vendor_file(vendor.type, vendor.path, id_column=id_column)


def _map_vendor_rows_to_loan_id(
    *,
    vendor: VendorInputConfig,
    extracted_df: pd.DataFrame,
    tape_loan_ids: set[str],
    collateral_to_loan_map_df: pd.DataFrame | None,
) -> _ProcessedVendor:
    if vendor.match_key == "loan_id":
        mapped_df = extracted_df.copy()
        mapped_df["loan_id"] = mapped_df["loan_id"].map(normalize_loan_id)

        vendor_id_set = set(mapped_df["loan_id"].dropna().tolist())
        matched_id_set = vendor_id_set & tape_loan_ids
        missing_ids = sorted(tape_loan_ids - vendor_id_set)
        extra_ids = sorted(vendor_id_set - tape_loan_ids)

        matched_df = mapped_df.loc[mapped_df["loan_id"].isin(tape_loan_ids)].copy()
    else:
        if collateral_to_loan_map_df is None:
            raise ValueError(
                f"Vendor '{vendor.name}' requires collateral_id mapping, but no collateral map is available."
            )

        mapped_df = extracted_df.merge(
            collateral_to_loan_map_df,
            on="collateral_id",
            how="left",
        )

        vendor_id_set = set(extracted_df["collateral_id"].dropna().tolist())
        extra_ids = sorted(
            mapped_df.loc[mapped_df["loan_id"].isna(), "collateral_id"].dropna().unique().tolist()
        )
        matched_df = mapped_df.loc[mapped_df["loan_id"].notna()].copy()
        matched_id_set = set(matched_df["loan_id"].dropna().tolist())
        missing_ids = sorted(tape_loan_ids - matched_id_set)

    duplicate_mapped_loans = sorted(
        matched_df.loc[matched_df["loan_id"].duplicated(keep=False), "loan_id"].dropna().unique().tolist()
    )
    if duplicate_mapped_loans:
        duplicate_summary = ", ".join(duplicate_mapped_loans)
        raise ValueError(
            f"Vendor '{vendor.name}' produced duplicate mapped loan_id values after "
            f"match_key='{vendor.match_key}': {duplicate_summary}"
        )

    qa_summary: dict[str, int | float] = {
        "vendor_rows": int(len(extracted_df)),
        "vendor_unique_ids": int(len(vendor_id_set)),
        "matched_loans": int(len(matched_id_set)),
        "match_rate": float(len(matched_id_set) / len(tape_loan_ids)) if tape_loan_ids else 0.0,
        "missing_in_vendor": int(len(missing_ids)),
        "extra_in_vendor": int(len(extra_ids)),
    }
    exceptions: dict[str, object] = {
        "missing_in_vendor": missing_ids,
        "extra_in_vendor": extra_ids,
        "missing_in_vendor_count": len(missing_ids),
        "extra_in_vendor_count": len(extra_ids),
    }
    return _ProcessedVendor(
        config=vendor,
        extracted_df=extracted_df.reset_index(drop=True),
        mapped_df=mapped_df.reset_index(drop=True),
        matched_df=matched_df.reset_index(drop=True),
        qa_summary=qa_summary,
        exceptions=exceptions,
    )


def _derive_hoa_flags_from_amounts(amounts: pd.Series) -> pd.Series:
    flags = pd.Series(["" for _ in range(len(amounts))], index=amounts.index, dtype=object)
    flags.loc[amounts > 0] = "Y"
    flags.loc[amounts == 0] = "N"
    return flags


def _should_default_blank_hoa_to_zero(vendor: VendorInputConfig) -> bool:
    return vendor.type.strip().lower() in {"dd_hoa", "consolidated_analytics"}


def _is_limited_review(value: object) -> bool:
    return isinstance(value, str) and value.strip().lower() == _LIMITED_REVIEW_STATUS


def _apply_limited_review_overrides(report_df: pd.DataFrame) -> None:
    if _REVIEW_STATUS_HEADER not in report_df.columns:
        return

    limited_review_mask = report_df[_REVIEW_STATUS_HEADER].map(_is_limited_review)
    if not bool(limited_review_mask.any()):
        return

    if _HOA_VALUE_HEADER not in report_df.columns:
        report_df[_HOA_VALUE_HEADER] = None
    if _HOA_FLAG_HEADER not in report_df.columns:
        report_df[_HOA_FLAG_HEADER] = ""

    report_df.loc[limited_review_mask, _HOA_FLAG_HEADER] = _LIMITED_REVIEW_HOA_FLAG
    report_df.loc[limited_review_mask, _HOA_VALUE_HEADER] = _LIMITED_REVIEW_HOA_PAYMENT


def _fill_report_from_vendor(
    *,
    report_df: pd.DataFrame,
    tape_ids: pd.Series,
    processed_vendor: _ProcessedVendor,
) -> dict[str, float]:
    matched_df = processed_vendor.matched_df
    if matched_df.empty:
        return {}

    if _HOA_VALUE_HEADER not in report_df.columns:
        report_df[_HOA_VALUE_HEADER] = None
    if _HOA_FLAG_HEADER not in report_df.columns:
        report_df[_HOA_FLAG_HEADER] = ""
    if "hoa_source_used" not in report_df.columns:
        report_df["hoa_source_used"] = None
    if "hoa_source_file_used" not in report_df.columns:
        report_df["hoa_source_file_used"] = None

    work_df = matched_df.copy()
    work_df["loan_id"] = work_df["loan_id"].map(normalize_loan_id)
    work_df["hoa_monthly_dues_amount"] = pd.to_numeric(
        work_df["hoa_monthly_dues_amount"],
        errors="coerce",
    )
    if _should_default_blank_hoa_to_zero(processed_vendor.config):
        # DD-matched rows with blank HOA should default to 0.0 in output.
        work_df["hoa_monthly_dues_amount"] = work_df["hoa_monthly_dues_amount"].fillna(0.0)

    loan_indexed = work_df.set_index("loan_id", drop=False)
    mapped_amount = tape_ids.map(loan_indexed["hoa_monthly_dues_amount"])

    payment_blank_mask = report_df[_HOA_VALUE_HEADER].map(_is_blank)
    fill_payment_mask = payment_blank_mask & mapped_amount.notna()
    report_df.loc[fill_payment_mask, _HOA_VALUE_HEADER] = mapped_amount.loc[fill_payment_mask]

    derived_flag = _derive_hoa_flags_from_amounts(mapped_amount)
    hoa_blank_mask = report_df[_HOA_FLAG_HEADER].map(_is_blank)
    fill_hoa_mask = hoa_blank_mask & mapped_amount.notna()
    report_df.loc[fill_hoa_mask, _HOA_FLAG_HEADER] = derived_flag.loc[fill_hoa_mask]

    contribution_mask = fill_payment_mask | fill_hoa_mask
    mapped_source = tape_ids.map(loan_indexed["hoa_source"])
    mapped_file = tape_ids.map(loan_indexed["hoa_source_file"])
    mapped_source.loc[mapped_source.map(_is_blank)] = processed_vendor.config.name

    source_blank_mask = report_df["hoa_source_used"].map(_is_blank) & contribution_mask
    source_file_blank_mask = report_df["hoa_source_file_used"].map(_is_blank) & contribution_mask
    report_df.loc[source_blank_mask, "hoa_source_used"] = mapped_source.loc[source_blank_mask]
    report_df.loc[source_file_blank_mask, "hoa_source_file_used"] = mapped_file.loc[source_file_blank_mask]

    non_null_amounts = work_df.loc[work_df["hoa_monthly_dues_amount"].notna(), ["loan_id", "hoa_monthly_dues_amount"]]
    return {
        str(row.loan_id): float(row.hoa_monthly_dues_amount)
        for row in non_null_amounts.itertuples(index=False)
    }


def _fill_report_from_clayton(
    *,
    report_df: pd.DataFrame,
    tape_ids: pd.Series,
    clayton_df: pd.DataFrame,
) -> pd.DataFrame:
    """Backward-compatible helper used by tests; delegates to blank-only fill semantics."""
    clayton_work_df = clayton_df.copy()
    if "hoa_source" not in clayton_work_df.columns:
        clayton_work_df["hoa_source"] = "CLAYTON"
    if "hoa_source_file" not in clayton_work_df.columns:
        clayton_work_df["hoa_source_file"] = None

    processed_vendor = _ProcessedVendor(
        config=VendorInputConfig(name="clayton", type="clayton", path=Path(""), match_key="loan_id"),
        extracted_df=clayton_work_df,
        mapped_df=clayton_work_df,
        matched_df=clayton_work_df,
        qa_summary={},
        exceptions={},
    )
    _fill_report_from_vendor(
        report_df=report_df,
        tape_ids=tape_ids.map(normalize_loan_id),
        processed_vendor=processed_vendor,
    )
    return report_df


def _append_vendor_qa_rows(
    base_qa_df: pd.DataFrame,
    processed_vendors: list[_ProcessedVendor],
) -> pd.DataFrame:
    if not processed_vendors:
        return base_qa_df

    vendor_rows: list[dict[str, object]] = []
    for processed_vendor in processed_vendors:
        vendor_name = processed_vendor.config.name
        summary = processed_vendor.qa_summary
        vendor_rows.extend(
            [
                {
                    "Metric": f"{vendor_name} - Vendor Rows",
                    "Value": summary["vendor_rows"],
                },
                {
                    "Metric": f"{vendor_name} - Vendor Unique IDs",
                    "Value": summary["vendor_unique_ids"],
                },
                {
                    "Metric": f"{vendor_name} - Matched Loans",
                    "Value": summary["matched_loans"],
                },
                {
                    "Metric": f"{vendor_name} - Match Rate",
                    "Value": summary["match_rate"],
                },
                {
                    "Metric": f"{vendor_name} - Missing in Vendor",
                    "Value": summary["missing_in_vendor"],
                },
                {
                    "Metric": f"{vendor_name} - Extra in Vendor",
                    "Value": summary["extra_in_vendor"],
                },
            ]
        )

    vendor_qa_df = pd.DataFrame(vendor_rows, columns=["Metric", "Value"])
    spacer = pd.DataFrame([{"Metric": "", "Value": ""}], columns=["Metric", "Value"])
    return pd.concat([base_qa_df, spacer, vendor_qa_df], ignore_index=True)


def _values_disagree(values: Sequence[float]) -> bool:
    return len({value for value in values}) > 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    if not config_path.exists():
        parser.error(f"Config file not found: {config_path}")

    try:
        config = load_config(config_path)
    except (OSError, ValueError) as exc:
        parser.error(f"Failed to load config: {exc}")

    effective_config = _build_effective_config(config, args)
    try:
        validate_paths(effective_config)
    except FileNotFoundError as exc:
        parser.error(str(exc))

    try:
        for vendor in effective_config.vendors:
            get_vendor_extractor(vendor.type)
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))

    print("Input path validation: OK")

    tape_df, tape_qa = extract_semt_tape(effective_config.tape_path)
    loan_master_df = tape_df
    if effective_config.run_sql:
        if effective_config.sql is None:
            parser.error("'sql' settings are required when 'run_sql' is true")
        try:
            sql_enrichment_df = run_sql_enrichment_query(
                tape_df=tape_df,
                connection_string=effective_config.sql.connection_string,
                query_path=effective_config.sql.query_path,
            )
            loan_master_df = merge_sql_enrichment_onto_tape(tape_df, sql_enrichment_df)
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            parser.error(f"SQL enrichment failed: {exc}")

    report_df = build_template_report_df(loan_master_df)
    tape_ids = loan_master_df["loan_id"].map(normalize_loan_id)
    invalid_tape_ids = int(tape_ids.isna().sum())
    if invalid_tape_ids:
        parser.error(f"Found {invalid_tape_ids} blank/unparseable tape loan_id values after enrichment")

    tape_loan_id_set = set(tape_ids.dropna().tolist())

    ordered_vendors: list[VendorInputConfig]
    try:
        ordered_vendors = _resolve_vendor_order(
            effective_config.vendors,
            effective_config.vendor_priority,
        )
    except ValueError as exc:
        parser.error(str(exc))

    requires_collateral_mapping = any(vendor.match_key == "collateral_id" for vendor in ordered_vendors)
    collateral_to_loan_map_df: pd.DataFrame | None = None
    if requires_collateral_mapping:
        try:
            collateral_to_loan_map_df = _build_collateral_to_loan_map(
                loan_ids=tape_ids,
                report_df=report_df,
            )
        except ValueError as exc:
            parser.error(str(exc))

    processed_vendors: list[_ProcessedVendor] = []
    loan_vendor_amounts: dict[str, list[float]] = {}
    for vendor in ordered_vendors:
        try:
            extracted_df = _extract_vendor_frame(vendor)
            processed_vendor = _map_vendor_rows_to_loan_id(
                vendor=vendor,
                extracted_df=extracted_df,
                tape_loan_ids=tape_loan_id_set,
                collateral_to_loan_map_df=collateral_to_loan_map_df,
            )
        except (TypeError, ValueError, KeyError) as exc:
            parser.error(f"Vendor '{vendor.name}' failed: {exc}")

        amount_map = _fill_report_from_vendor(
            report_df=report_df,
            tape_ids=tape_ids,
            processed_vendor=processed_vendor,
        )
        for loan_id, amount in amount_map.items():
            loan_vendor_amounts.setdefault(loan_id, []).append(amount)

        processed_vendors.append(processed_vendor)

    discrepant_loan_ids = {
        loan_id for loan_id, amounts in loan_vendor_amounts.items() if _values_disagree(amounts)
    }
    report_df["hoa_discrepancy_flag"] = tape_ids.map(
        lambda loan_id: bool(loan_id in discrepant_loan_ids) if loan_id is not None else False
    )
    _apply_limited_review_overrides(report_df)

    qa_merged_df = pd.DataFrame(
        {
            "loan_id": tape_ids,
            "hoa_monthly_dues_amount": report_df[_HOA_VALUE_HEADER] if _HOA_VALUE_HEADER in report_df.columns else None,
        },
        dtype=object,
    )
    vendor_frames_for_qa = [processed_vendor.mapped_df for processed_vendor in processed_vendors]
    qa_df, qa_dict = compute_qa(
        tape_df=tape_df,
        vendor_df=vendor_frames_for_qa,
        merged_df=qa_merged_df,
        tape_raw_rows=int(tape_qa.get("input_row_count", len(tape_df))),
    )
    qa_df = _append_vendor_qa_rows(qa_df, processed_vendors)

    vendor_exceptions = {
        processed_vendor.config.name: processed_vendor.exceptions
        for processed_vendor in processed_vendors
    }

    output_report_df = report_df.loc[:, TEMPLATE_REPORT_COLUMNS].copy()

    try:
        output_path = write_report_from_template(
            template_path=effective_config.template_path,
            output_path=effective_config.output_path,
            report_df=output_report_df,
            qa_df=qa_df,
            exceptions=vendor_exceptions,
        )
    except PermissionError:
        parser.error(
            "Failed to write output workbook at "
            f"'{effective_config.output_path}': permission denied. "
            "Close the file if it is open in Excel or use --out to write to a different path."
        )
    except OSError as exc:
        parser.error(f"Failed to write output workbook at '{effective_config.output_path}': {exc}")

    print(f"Output written to: {output_path}")
    _print_qa_summary(qa_dict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
