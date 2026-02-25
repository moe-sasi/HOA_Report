"""Vendor-specific extractors.

Each vendor should get its own module file in this package.
"""

from hoa_report.extractors.semt import extract_semt_tape

__all__ = ["extract_semt_tape"]
