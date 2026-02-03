"""CSV file adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from datawash.adapters.base import register_adapter


class CsvAdapter:
    def read(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        return pd.read_csv(path, **kwargs)

    def write(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        df.to_csv(path, index=False, **kwargs)


_adapter = CsvAdapter()
register_adapter("csv", _adapter)
register_adapter("tsv", _adapter)  # TSV uses same adapter with sep='\t'
