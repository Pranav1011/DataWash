"""Base transformer interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult


class BaseTransformer(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique transformer name."""

    @abstractmethod
    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        """Apply transformation. Returns (new_df, result). Must NOT mutate input."""

    @abstractmethod
    def generate_code(self, **params: Any) -> str:
        """Return equivalent pandas code string."""
