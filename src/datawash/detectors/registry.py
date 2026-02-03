"""Detector registration and orchestration."""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from datawash.core.models import DatasetProfile, Finding
from datawash.detectors.base import BaseDetector

logger = logging.getLogger(__name__)

_DETECTORS: dict[str, BaseDetector] = {}


def register_detector(detector: BaseDetector) -> None:
    _DETECTORS[detector.name] = detector


def get_all_detectors() -> dict[str, BaseDetector]:
    return dict(_DETECTORS)


def run_all_detectors(
    df: pd.DataFrame,
    profile: DatasetProfile,
    enabled: Optional[list[str]] = None,
) -> list[Finding]:
    """Run enabled detectors and return all findings."""
    findings: list[Finding] = []
    for name, detector in _DETECTORS.items():
        if enabled is not None and name not in enabled:
            continue
        try:
            logger.info("Running detector: %s", name)
            results = detector.detect(df, profile)
            findings.extend(results)
            logger.info("Detector %s found %d issues", name, len(results))
        except Exception:
            logger.exception("Detector %s failed", name)
    return findings
