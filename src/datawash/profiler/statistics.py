"""Statistical computations for column profiling."""

from __future__ import annotations

from typing import Any

import pandas as pd


def compute_numeric_stats(series: pd.Series) -> dict[str, Any]:
    """Compute statistics for a numeric column."""
    clean = series.dropna()
    if clean.empty:
        return {}
    return {
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "std": float(clean.std()) if len(clean) > 1 else 0.0,
        "min": float(clean.min()),
        "max": float(clean.max()),
        "q25": float(clean.quantile(0.25)),
        "q75": float(clean.quantile(0.75)),
        "skewness": float(clean.skew()) if len(clean) > 2 else 0.0,
        "kurtosis": float(clean.kurtosis()) if len(clean) > 3 else 0.0,
    }


def compute_categorical_stats(series: pd.Series) -> dict[str, Any]:
    """Compute statistics for a categorical/string column."""
    clean = series.dropna()
    if clean.empty:
        return {}
    value_counts = clean.value_counts()
    top_n = value_counts.head(10)
    return {
        "top_values": {str(k): int(v) for k, v in top_n.items()},
        "mode": str(value_counts.index[0]) if len(value_counts) > 0 else None,
        "avg_length": float(clean.astype(str).str.len().mean()),
        "min_length": int(clean.astype(str).str.len().min()),
        "max_length": int(clean.astype(str).str.len().max()),
    }
