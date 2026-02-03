"""JSON file adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from datawash.adapters.base import register_adapter


class JsonAdapter:
    def read(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        return pd.read_json(path, **kwargs)

    def write(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        df.to_json(path, orient="records", indent=2, **kwargs)


register_adapter("json", JsonAdapter())
