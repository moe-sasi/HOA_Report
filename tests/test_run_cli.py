from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from openpyxl import Workbook

from hoa_report.engine import TEMPLATE_REPORT_COLUMNS
from hoa_report.run import main

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_config(filename: str, payload: dict[str, object]) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    config_path = _TEST_TMP_DIR / filename
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def _write_tape_fixture(filename: str) -> Path:
    tape_path = _TEST_TMP_DIR / filename
    df = pd.DataFrame({"Loan Number": ["L-1001", "L-1002", "L-1003"]})
    df.to_excel(tape_path, index=False)
    return tape_path


def _write_vendor_fixture(filename: str, rows: list[dict[str, object]]) -> Path:
    vendor_path = _TEST_TMP_DIR / filename
    pd.DataFrame(rows).to_excel(vendor_path, index=False)
    return vendor_path


def _write_template_fixture(filename: str) -> Path:
    template_path = _TEST_TMP_DIR / filename
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Sheet1"
    for col_idx, header in enumerate(TEMPLATE_REPORT_COLUMNS, start=1):
        sheet.cell(row=1, column=col_idx, value=header)
    workbook.save(template_path)
    return template_path


def test_cli_runs_end_to_end_with_path_overrides_and_prints_qa_summary(capsys: pytest.CaptureFixture[str]) -> None:
    tape_path = _write_tape_fixture("run_cli.tape.synthetic.xlsx")
    template_path = _write_template_fixture("run_cli.template.synthetic.xlsx")
    vendor_a_path = _write_vendor_fixture(
        "run_cli.vendor_a.synthetic.xlsx",
        [
            {"Loan Number": "L-1001", "Monthly Dues": 125.0},
            {"Loan Number": "L-1002", "Monthly Dues": 225.0},
        ],
    )
    vendor_b_path = _write_vendor_fixture(
        "run_cli.vendor_b.synthetic.xlsx",
        [
            {"Loan Number": "L-1003", "Monthly Dues": 300.0},
        ],
    )

    config_path = _write_config(
        "run_cli.override.config.json",
        {
            "tape_path": "tests/fixtures/does-not-exist.xlsx",
            "template_path": "tests/fixtures/does-not-exist-template.xlsx",
            "vendor_paths": ["tests/fixtures/does-not-exist-vendor.xlsx"],
            "vendor_type": "example_vendor",
            "output_path": "data/_test_tmp/should-not-be-used.xlsx",
        },
    )
    output_path = _TEST_TMP_DIR / f"run_cli.override.output.{uuid4().hex}.xlsx"

    exit_code = main(
        [
            "--config",
            str(config_path),
            "--tape-path",
            str(tape_path),
            "--template-path",
            str(template_path),
            "--vendor-path",
            str(vendor_a_path),
            "--vendor-path",
            str(vendor_b_path),
            "--out",
            str(output_path),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert output_path.exists()
    assert "Input path validation: OK" in captured.out
    assert "QA Summary" in captured.out
    assert "Match Rate" in captured.out


def test_cli_errors_when_override_path_does_not_exist(capsys: pytest.CaptureFixture[str]) -> None:
    config_path = _write_config(
        "run_cli.invalid_path.config.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendor_paths": ["tests/fixtures/vendor_a.synthetic.xlsx"],
            "vendor_type": "example_vendor",
            "output_path": "data/_test_tmp/run_cli.invalid.output.xlsx",
        },
    )

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--config",
                str(config_path),
                "--vendor-path",
                "tests/fixtures/does-not-exist-vendor.xlsx",
            ]
        )

    captured = capsys.readouterr()
    assert exc_info.value.code == 2
    assert "do not exist" in captured.err
