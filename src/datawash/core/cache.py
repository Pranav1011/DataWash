"""Lazy-evaluated computation cache shared across detectors."""

from __future__ import annotations

from typing import Any

import pandas as pd


class ComputationCache:
    """Cache expensive column computations.

    Computes on first access, returns cached value on subsequent access.
    All detectors share the same cache instance to avoid redundant work.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._null_masks: dict[str, pd.Series] = {}
        self._value_sets: dict[str, set[str]] = {}
        self._unique_counts: dict[str, int] = {}
        self._statistics: dict[str, dict[str, Any]] = {}

    def get_null_mask(self, column: str) -> pd.Series:
        """Return boolean mask of null values. Cached."""
        if column not in self._null_masks:
            self._null_masks[column] = self._df[column].isna()
        return self._null_masks[column]

    def get_value_set(self, column: str, max_values: int = 10000) -> set[str]:
        """Return set of unique non-null string values. Cached."""
        if column not in self._value_sets:
            values = self._df[column].dropna()
            if len(values) > max_values:
                values = values.sample(max_values, random_state=42)
            self._value_sets[column] = set(values.astype(str))
        return self._value_sets[column]

    def get_unique_count(self, column: str) -> int:
        """Return count of unique values. Cached."""
        if column not in self._unique_counts:
            self._unique_counts[column] = int(self._df[column].nunique())
        return self._unique_counts[column]

    def get_statistics(self, column: str) -> dict[str, Any]:
        """Return numeric statistics. Cached."""
        if column not in self._statistics:
            col = self._df[column]
            if pd.api.types.is_numeric_dtype(col):
                clean = col.dropna()
                if clean.empty:
                    self._statistics[column] = {}
                else:
                    self._statistics[column] = {
                        "mean": float(clean.mean()),
                        "std": float(clean.std()) if len(clean) > 1 else 0.0,
                        "min": float(clean.min()),
                        "max": float(clean.max()),
                        "q1": float(clean.quantile(0.25)),
                        "q3": float(clean.quantile(0.75)),
                    }
            else:
                self._statistics[column] = {}
        return self._statistics[column]
