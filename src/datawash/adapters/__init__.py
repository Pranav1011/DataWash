"""Data adapters for loading and saving DataFrames."""

# Import adapters to trigger registration
from datawash.adapters import (
    csv_adapter,  # noqa: F401
    excel_adapter,  # noqa: F401
    json_adapter,  # noqa: F401
    parquet_adapter,  # noqa: F401
)
from datawash.adapters.base import load_dataframe

__all__ = ["load_dataframe"]
