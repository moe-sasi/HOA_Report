from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Protocol

import pandas as pd


class hoa_vendor_extractor(Protocol):
    """Protocol contract for vendor extractor implementations."""

    def extract(self, path: str | Path) -> pd.DataFrame:
        """Read one vendor file path and return raw extractor rows."""
        ...


VendorExtractorFn = Callable[[str | Path], pd.DataFrame]
