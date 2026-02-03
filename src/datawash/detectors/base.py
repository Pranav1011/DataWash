"""Base detector interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from datawash.core.models import DatasetProfile, Finding


class BaseDetector(ABC):
    """Abstract base class for all detectors."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique detector name."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description."""

    @abstractmethod
    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        """Run detection and return findings."""
