from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from hoa_report.config import InputConfig, load_config, validate_paths
from hoa_report.engine import build_template_report_df, merge_hoa_sources
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
_CLAYTON_QA_PRINT_ORDER: tuple[tuple[str, str], ...] = (
    ("tape_unique_loans", "Tape Unique Loans"),
    ("clayton_rows", "Clayton Rows"),
    ("clayton_unique_loans", "Clayton Unique Loans"),
    ("matched_loans", "Matched Loans"),
    ("match_rate", "Match Rate"),
    ("missing_in_clayton", "Missing in Clayton"),
    ("extra_in_clayton", "Extra in Clayton"),
)


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
        help="Override vendor extractor type from config (registry key)",
    )
    return parser


def _build_effective_config(config: InputConfig, args: argparse.Namespace) -> InputConfig:
    tape_path = Path(args.tape_path) if args.tape_path else config.tape_path
    template_path = Path(args.template_path) if args.template_path else config.template_path
    vendor_paths = [Path(path) for path in args.vendor_paths] if args.vendor_paths else config.vendor_paths
    output_path = Path(args.out) if args.out else config.output_path
    vendor_type = args.vendor_type or config.vendor_type

    return InputConfig(
        tape_path=tape_path,
        template_path=template_path,
        vendor_paths=vendor_paths,
        vendor_type=vendor_type,
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


def _combine_vendor_frames(vendor_frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not vendor_frames:
        return pd.DataFrame(columns=["loan_id", "hoa_monthly_dues_amount"], dtype=object)

    combined = pd.concat(vendor_frames, ignore_index=True).copy()
    if "loan_id" not in combined.columns:
        raise ValueError("Clayton merge requires extracted vendor rows to contain 'loan_id'.")

    combined["loan_id"] = combined["loan_id"].map(normalize_loan_id)
    combined = combined.loc[combined["loan_id"].notna()].copy()

    duplicate_ids = sorted(combined.loc[combined["loan_id"].duplicated(keep=False), "loan_id"].unique())
    if duplicate_ids:
        duplicate_summary = ", ".join(duplicate_ids)
        raise ValueError(
            "Clayton merge requires unique normalized loan_id values across all provided "
            f"vendor files. Duplicates found: {duplicate_summary}"
        )

    return combined.reset_index(drop=True)


def _build_clayton_qa(
    *,
    tape_ids: pd.Series,
    clayton_df: pd.DataFrame,
) -> tuple[dict[str, int | float], dict[str, dict[str, object]]]:
    normalized_tape_ids = tape_ids.map(normalize_loan_id).dropna()
    tape_unique_ids = set(normalized_tape_ids.tolist())
    clayton_ids = set(clayton_df["loan_id"].map(normalize_loan_id).dropna().tolist())

    matched = tape_unique_ids & clayton_ids
    missing = sorted(tape_unique_ids - clayton_ids)
    extra = sorted(clayton_ids - tape_unique_ids)
    summary: dict[str, int | float] = {
        "tape_unique_loans": int(len(tape_unique_ids)),
        "clayton_rows": int(len(clayton_df)),
        "clayton_unique_loans": int(len(clayton_ids)),
        "matched_loans": int(len(matched)),
        "match_rate": float(len(matched) / len(tape_unique_ids)) if tape_unique_ids else 0.0,
        "missing_in_clayton": int(len(missing)),
        "extra_in_clayton": int(len(extra)),
    }
    exceptions = {
        "CLAYTON": {
            "missing_in_vendor": missing,
            "extra_in_vendor": extra,
            "missing_in_vendor_count": len(missing),
            "extra_in_vendor_count": len(extra),
        }
    }
    return summary, exceptions


def _print_clayton_qa_summary(summary: dict[str, int | float]) -> None:
    print("Clayton Match Summary")
    for metric_key, label in _CLAYTON_QA_PRINT_ORDER:
        print(
            f"- {metric_key} ({label}): "
            f"{_format_qa_value(metric_key, summary.get(metric_key, ''))}"
        )


def _fill_report_from_clayton(
    *,
    report_df: pd.DataFrame,
    tape_ids: pd.Series,
    clayton_df: pd.DataFrame,
) -> pd.DataFrame:
    if report_df.empty or clayton_df.empty:
        return report_df

    if "HOA Monthly Payment" not in report_df.columns:
        report_df["HOA Monthly Payment"] = None
    if "HOA" not in report_df.columns:
        report_df["HOA"] = ""

    clayton_map = clayton_df.set_index("loan_id")["hoa_monthly_dues_amount"]
    mapped_payment = tape_ids.map(normalize_loan_id).map(clayton_map)
    numeric_payment = pd.to_numeric(mapped_payment, errors="coerce")

    payment_blank_mask = report_df["HOA Monthly Payment"].map(_is_blank)
    fill_payment_mask = payment_blank_mask & numeric_payment.notna()
    report_df.loc[fill_payment_mask, "HOA Monthly Payment"] = numeric_payment.loc[fill_payment_mask]

    derived_flag = pd.Series([""] * len(report_df), index=report_df.index, dtype=object)
    derived_flag.loc[numeric_payment > 0] = "Y"
    derived_flag.loc[numeric_payment == 0] = "N"

    hoa_blank_mask = report_df["HOA"].map(_is_blank)
    fill_hoa_mask = hoa_blank_mask & numeric_payment.notna()
    report_df.loc[fill_hoa_mask, "HOA"] = derived_flag.loc[fill_hoa_mask]
    return report_df


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

    vendor_type = effective_config.vendor_type
    try:
        get_vendor_extractor(vendor_type)
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

    vendor_frames = [
        extract_vendor_file(vendor_type, vendor_path) for vendor_path in effective_config.vendor_paths
    ]

    if vendor_type.strip().lower() == "clayton":
        clayton_df = _combine_vendor_frames(vendor_frames)
        merged_df, _ignored_vendor_exceptions = merge_hoa_sources(
            loan_master_df=loan_master_df,
            hoa_sources=[],
            priority=[],
        )
        report_df = build_template_report_df(merged_df)
        report_df = _fill_report_from_clayton(
            report_df=report_df,
            tape_ids=merged_df["loan_id"],
            clayton_df=clayton_df,
        )
        clayton_summary, vendor_exceptions = _build_clayton_qa(
            tape_ids=merged_df["loan_id"],
            clayton_df=clayton_df,
        )
        _print_clayton_qa_summary(clayton_summary)
        qa_merged_df = merged_df.copy()
        qa_merged_df["hoa_monthly_dues_amount"] = report_df["HOA Monthly Payment"]
    else:
        merged_df, vendor_exceptions = merge_hoa_sources(
            loan_master_df=loan_master_df,
            hoa_sources=vendor_frames,
            priority=[vendor_type],
        )
        report_df = build_template_report_df(merged_df)
        qa_merged_df = merged_df

    qa_df, qa_dict = compute_qa(
        tape_df=tape_df,
        vendor_df=vendor_frames,
        merged_df=qa_merged_df,
        tape_raw_rows=int(tape_qa.get("input_row_count", len(tape_df))),
    )
    try:
        output_path = write_report_from_template(
            template_path=effective_config.template_path,
            output_path=effective_config.output_path,
            report_df=report_df,
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
