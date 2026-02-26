from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Sequence

from hoa_report.config import InputConfig, load_config, validate_paths
from hoa_report.engine import build_template_report_df, merge_hoa_sources
from hoa_report.extractors import extract_semt_tape, extract_vendor_file, get_vendor_extractor
from hoa_report.io import write_report_from_template
from hoa_report.qa import compute_qa
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
    merged_df, vendor_exceptions = merge_hoa_sources(
        loan_master_df=loan_master_df,
        hoa_sources=vendor_frames,
        priority=[vendor_type],
    )
    report_df = build_template_report_df(merged_df)
    qa_df, qa_dict = compute_qa(
        tape_df=tape_df,
        vendor_df=vendor_frames,
        merged_df=merged_df,
        tape_raw_rows=int(tape_qa.get("input_row_count", len(tape_df))),
    )
    output_path = write_report_from_template(
        template_path=effective_config.template_path,
        output_path=effective_config.output_path,
        report_df=report_df,
        qa_df=qa_df,
        exceptions=vendor_exceptions,
    )

    print(f"Output written to: {output_path}")
    _print_qa_summary(qa_dict)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
