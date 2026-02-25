# HOA_Report

Generate HOA report from DD reports.

## Local run

Use a local config file with private file paths (kept out of git):

```bash
python -m hoa_report.run --config config/example.local.json
```

`vendor_type` in config selects the vendor extractor registry key (override with `--vendor-type`).
