"""Excel file adapter (requires openpyxl)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from datawash.adapters.base import register_adapter
from datawash.core.exceptions import AdapterError


class ExcelAdapter:
    def read(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        try:
            return pd.read_excel(path, **kwargs)
        except ImportError:
            raise AdapterError(
                "Excel support requires openpyxl. "
                "Install with: pip install datawash[formats]"
            )

    def write(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        try:
            df.to_excel(path, index=False, **kwargs)
        except ImportError:
            raise AdapterError(
                "Excel support requires openpyxl. "
                "Install with: pip install datawash[formats]"
            )


_adapter = ExcelAdapter()
register_adapter("xlsx", _adapter)
register_adapter("xls", _adapter)
