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
