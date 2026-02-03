"""Main profiling orchestrator."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from datawash.core.models import ColumnProfile, DatasetProfile
from datawash.profiler.patterns import detect_column_patterns
from datawash.profiler.statistics import (
    compute_categorical_stats,
    compute_numeric_stats,
)

logger = logging.getLogger(__name__)


def profile_dataset(df: pd.DataFrame) -> DatasetProfile:
    """Generate a complete profile for a DataFrame.

    Args:
        df: The DataFrame to profile.

    Returns:
        DatasetProfile with column-level and dataset-level statistics.
    """
    logger.info("Profiling dataset: %d rows, %d columns", len(df), len(df.columns))

    columns: dict[str, ColumnProfile] = {}
    for col_name in df.columns:
        columns[col_name] = _profile_column(df[col_name])

    return DatasetProfile(
        row_count=len(df),
        column_count=len(df.columns),
        memory_bytes=int(df.memory_usage(deep=True).sum()),
        columns=columns,
        duplicate_row_count=int(df.duplicated().sum()),
    )


def _profile_column(series: pd.Series) -> ColumnProfile:
    """Profile a single column."""
    name = str(series.name)
    null_count = int(series.isna().sum())
    total = len(series)
    unique_count = int(series.nunique())

    # Compute type-appropriate statistics
    stats: dict[str, Any] = {}
    if pd.api.types.is_numeric_dtype(series):
        stats = compute_numeric_stats(series)
    else:
        stats = compute_categorical_stats(series)

    # Detect patterns
    patterns = detect_column_patterns(series)

    # Sample values (up to 5 non-null)
    sample_values = series.dropna().head(5).tolist()

    return ColumnProfile(
        name=name,
        dtype=str(series.dtype),
        null_count=null_count,
        null_ratio=round(null_count / total, 4) if total > 0 else 0.0,
        unique_count=unique_count,
        unique_ratio=round(unique_count / total, 4) if total > 0 else 0.0,
        sample_values=sample_values,
        statistics=stats,
        patterns=patterns,
    )
