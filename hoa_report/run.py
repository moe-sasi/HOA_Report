from __future__ import annotations

import argparse
from pathlib import Path

from hoa_report.config import load_config, validate_paths
from hoa_report.extractors import get_vendor_extractor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run HOA report pipeline")
    parser.add_argument("--config", required=True, help="Path to local JSON config")
    parser.add_argument(
        "--vendor-type",
        help="Override vendor extractor type from config (registry key)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        parser.error(f"Config file not found: {config_path}")

    config = load_config(config_path)
    validate_paths(config)

    vendor_type = args.vendor_type or config.vendor_type
    try:
        get_vendor_extractor(vendor_type)
    except (KeyError, ValueError) as exc:
        parser.error(str(exc))

    print("Configuration and input paths validated successfully.")
    print(f"Vendor extractor selected: {vendor_type.strip().lower()}")
    print(f"Output will be written to: {config.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
