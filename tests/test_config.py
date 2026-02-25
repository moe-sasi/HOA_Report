from __future__ import annotations

import json
from pathlib import Path

from hoa_report.config import load_config

_TEST_TMP_DIR = Path("data/_test_tmp")


def _write_config(filename: str, payload: dict[str, object]) -> Path:
    _TEST_TMP_DIR.mkdir(parents=True, exist_ok=True)
    config_path = _TEST_TMP_DIR / filename
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def test_load_config_reads_vendor_type_from_config() -> None:
    config_path = _write_config(
        "config.vendor_type.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendor_paths": ["tests/fixtures/vendor_a.synthetic.xlsx"],
            "vendor_type": "example_vendor",
        },
    )

    config = load_config(config_path)
    assert config.vendor_type == "example_vendor"


def test_load_config_defaults_vendor_type_when_missing() -> None:
    config_path = _write_config(
        "config.default_vendor_type.json",
        {
            "tape_path": "tests/fixtures/tape.synthetic.xlsx",
            "template_path": "tests/fixtures/template.synthetic.xlsx",
            "vendor_paths": [],
        },
    )

    config = load_config(config_path)
    assert config.vendor_type == "example_vendor"
