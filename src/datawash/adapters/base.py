"""Base adapter interface and loader dispatch."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional, Protocol

import pandas as pd

from datawash.core.exceptions import AdapterError

logger = logging.getLogger(__name__)


class DataAdapter(Protocol):
    """Protocol for data adapters."""

    def read(self, path: Path, **kwargs: Any) -> pd.DataFrame: ...
    def write(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None: ...


_ADAPTERS: dict[str, DataAdapter] = {}


def register_adapter(extension: str, adapter: DataAdapter) -> None:
    _ADAPTERS[extension] = adapter


def load_dataframe(
    source: str | Path, format: Optional[str] = None, **kwargs: Any
) -> pd.DataFrame:
    """Load a DataFrame from a file path.

    Args:
        source: Path to the data file.
        format: Force file format. Auto-detected from extension if None.
        **kwargs: Passed to the adapter's read method.

    Returns:
        Loaded DataFrame.

    Raises:
        AdapterError: If the file cannot be loaded.
    """
    path = Path(source)
    if not path.exists():
        raise AdapterError(f"File not found: {path}")

    ext = format or path.suffix.lstrip(".")
    adapter = _ADAPTERS.get(ext)
    if adapter is None:
        raise AdapterError(
            f"Unsupported format: '{ext}'. "
            f"Supported formats: {', '.join(sorted(_ADAPTERS.keys()))}"
        )

    try:
        logger.info("Loading %s with %s adapter", path, ext)
        df = adapter.read(path, **kwargs)
        logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))
        return df
    except AdapterError:
        raise
    except Exception as e:
        raise AdapterError(f"Failed to read {path}: {e}") from e
