"""Detector registration and orchestration."""

from __future__ import annotations

import logging
import sys
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
    active_detectors = {
        n: d for n, d in _DETECTORS.items() if enabled is None or n in enabled
    }
    use_progress = len(df) > 10000 and sys.stderr.isatty()

    if use_progress:
        from rich.progress import Progress

        with Progress() as progress:
            task = progress.add_task(
                "Running detectors...", total=len(active_detectors)
            )
            for name, detector in active_detectors.items():
                try:
                    logger.info("Running detector: %s", name)
                    results = detector.detect(df, profile)
                    findings.extend(results)
                    logger.info("Detector %s found %d issues", name, len(results))
                except Exception:
                    logger.exception("Detector %s failed", name)
                progress.update(task, advance=1)
    else:
        for name, detector in active_detectors.items():
            try:
                logger.info("Running detector: %s", name)
                results = detector.detect(df, profile)
                findings.extend(results)
                logger.info("Detector %s found %d issues", name, len(results))
            except Exception:
                logger.exception("Detector %s failed", name)
    return findings
