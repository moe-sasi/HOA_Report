"""Vendor-specific extractors.

Each vendor should get its own module file in this package.
"""

from hoa_report.extractors.example_vendor import extract_example_vendor
from hoa_report.extractors.registry import (
    extract_vendor_file,
    get_vendor_extractor,
    list_vendor_extractors,
    register_vendor_extractor,
)
from hoa_report.extractors.semt import extract_semt_tape

__all__ = [
    "extract_example_vendor",
    "extract_semt_tape",
    "extract_vendor_file",
    "get_vendor_extractor",
    "list_vendor_extractors",
    "register_vendor_extractor",
]
