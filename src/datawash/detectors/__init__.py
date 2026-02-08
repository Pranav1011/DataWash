"""Detectors for data quality issues."""

# Import detectors to trigger registration (explicit re-exports for ruff)
from . import duplicate_detector as duplicate_detector
from . import format_detector as format_detector
from . import missing_detector as missing_detector
from . import outlier_detector as outlier_detector
from . import pii_detector as pii_detector
from . import similarity_detector as similarity_detector
from . import type_detector as type_detector
from .registry import get_all_detectors as get_all_detectors
from .registry import run_all_detectors as run_all_detectors
