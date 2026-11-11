"""DataFrame dtype optimization for performance."""

from __future__ import annotations

import pandas as pd


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame dtypes for faster analysis.

    Downcasts numeric types to reduce memory usage.
    Object/string columns are left unchanged to preserve detector compatibility.
    """
    df = df.copy()

    for col in df.columns:
        dtype = df[col].dtype

        if pd.api.types.is_integer_dtype(dtype):
            df[col] = pd.to_numeric(df[col], downcast="integer")
        elif pd.api.types.is_float_dtype(dtype):
            df[col] = pd.to_numeric(df[col], downcast="float")

    return df


def format_bytes(n: int) -> str:
    """Human-readable size: '3.3 KB', not '0.0 MB'."""
    size = float(n)
    for unit in ("B", "KB", "MB"):
        if round(size, 1) < 1024:
            return f"{size:.0f} B" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"
