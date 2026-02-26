# HOA_Report

Generate HOA report from DD reports.

## Local run

Use a local config file with your runtime paths:

```bash
python -m hoa_report.run --config config/local.json
```

Path overrides are available at runtime:

```bash
python -m hoa_report.run --config config/local.json \
  --tape-path "C:/private/tape.xlsx" \
  --template-path "C:/private/template.xlsx" \
  --vendor-path "C:/private/vendor_a.xlsx" \
  --vendor-path "C:/private/vendor_b.xlsx" \
  --out "C:/private/output/hoa_report.xlsx"
```

`vendor_type` in config selects the vendor extractor registry key (override with `--vendor-type`).
