"""Parallel column profiling and detector execution."""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import pandas as pd

from datawash.core.cache import ComputationCache
from datawash.core.models import ColumnProfile, DatasetProfile, Finding
from datawash.detectors.base import BaseDetector
from datawash.profiler.patterns import detect_column_patterns
from datawash.profiler.statistics import (
    compute_categorical_stats,
    compute_numeric_stats,
)

logger = logging.getLogger(__name__)

MAX_WORKERS = min(8, os.cpu_count() or 4)


def profile_dataset_parallel(
    df: pd.DataFrame,
    cache: Optional[ComputationCache] = None,
) -> DatasetProfile:
    """Profile all columns in parallel using ThreadPoolExecutor."""
    columns: dict[str, ColumnProfile] = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_col = {
            executor.submit(_profile_column, df[col_name]): col_name
            for col_name in df.columns
        }
        for future in as_completed(future_to_col):
            col_name = future_to_col[future]
            try:
                columns[col_name] = future.result()
            except Exception:
                logger.exception("Failed to profile column %s", col_name)
                columns[col_name] = _empty_profile(col_name)

    return DatasetProfile(
        row_count=len(df),
        column_count=len(df.columns),
        memory_bytes=int(df.memory_usage(deep=True).sum()),
        columns=columns,
        duplicate_row_count=int(df.duplicated().sum()),
    )


def run_detectors_parallel(
    df: pd.DataFrame,
    profile: DatasetProfile,
    detectors: dict[str, BaseDetector],
) -> list[Finding]:
    """Run all detectors in parallel."""
    findings: list[Finding] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_name = {
            executor.submit(detector.detect, df, profile): name
            for name, detector in detectors.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results = future.result()
                findings.extend(results)
            except Exception:
                logger.exception("Detector %s failed", name)

    return findings


def _profile_column(series: pd.Series) -> ColumnProfile:
    """Profile a single column (runs inside thread)."""
    name = str(series.name)
    null_count = int(series.isna().sum())
    total = len(series)
    unique_count = int(series.nunique())

    stats: dict[str, Any] = {}
    if pd.api.types.is_bool_dtype(series):
        stats = compute_categorical_stats(series)
    elif pd.api.types.is_numeric_dtype(series):
        stats = compute_numeric_stats(series)
    else:
        stats = compute_categorical_stats(series)

    patterns = detect_column_patterns(series)
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


def _empty_profile(col_name: str) -> ColumnProfile:
    """Fallback profile when profiling fails."""
    return ColumnProfile(
        name=col_name,
        dtype="unknown",
        null_count=0,
        null_ratio=0.0,
        unique_count=0,
        unique_ratio=0.0,
        sample_values=[],
        statistics={},
        patterns={},
    )
