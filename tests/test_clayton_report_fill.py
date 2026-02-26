from __future__ import annotations

import pandas as pd

from hoa_report.run import _fill_report_from_clayton


def test_fill_report_from_clayton_only_populates_blank_fields() -> None:
    report_df = pd.DataFrame(
        {
            "SEMT ID": ["L-1", "L-2", "L-3", "L-4"],
            "HOA": ["", "Y", "", "N"],
            "HOA Monthly Payment": [None, 999.0, None, None],
        }
    )
    tape_ids = pd.Series(["L-1", "L-2", "L-3", "L-4"], dtype=object)
    clayton_df = pd.DataFrame(
        {
            "loan_id": ["L1", "L2", "L3", "L4"],
            "hoa_monthly_dues_amount": [125.0, 0.0, None, 300.0],
        }
    )

    filled = _fill_report_from_clayton(report_df=report_df.copy(), tape_ids=tape_ids, clayton_df=clayton_df)

    payments = filled["HOA Monthly Payment"].tolist()
    assert payments[0] == 125.0
    assert payments[1] == 999.0
    assert pd.isna(payments[2])
    assert payments[3] == 300.0
    assert filled["HOA"].tolist() == ["Y", "Y", "", "N"]
