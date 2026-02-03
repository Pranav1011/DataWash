"""Parquet file adapter (requires pyarrow)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from datawash.adapters.base import register_adapter
from datawash.core.exceptions import AdapterError


class ParquetAdapter:
    def read(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        try:
            return pd.read_parquet(path, **kwargs)
        except ImportError:
            raise AdapterError(
                "Parquet support requires pyarrow. "
                "Install with: pip install datawash[formats]"
            )

    def write(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        try:
            df.to_parquet(path, index=False, **kwargs)
        except ImportError:
            raise AdapterError(
                "Parquet support requires pyarrow. "
                "Install with: pip install datawash[formats]"
            )


register_adapter("parquet", ParquetAdapter())
