"""Detectors for data quality issues."""

# Import detectors to trigger registration
from . import (  # noqa: F401
    duplicate_detector,
    format_detector,
    missing_detector,
    outlier_detector,
    similarity_detector,
    type_detector,
)
from .registry import get_all_detectors as get_all_detectors
from .registry import run_all_detectors as run_all_detectors
