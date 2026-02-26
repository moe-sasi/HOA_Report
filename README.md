# HOA_Report

Generate HOA report from DD reports.

## Local run

Use a local config file with your runtime paths:

```bash
python -m hoa_report.run --config config/local.json
```

To enable SQL enrichment, set `run_sql` to `true` and provide a SQLAlchemy
`mssql+pyodbc` connection string in config:

```json
{
  "run_sql": true,
  "sql": {
    "connection_string": "mssql+pyodbc://@RTSQLGEN01/LOANDATA?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes",
    "query_path": "sql/hoa_enrich.sql"
  }
}
```

SQL enrichment requires `pyodbc` (`sqlalchemy` is optional and will be used when installed).

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
