"""Tests for data adapters."""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import pytest
from datawash.adapters import load_dataframe
from datawash.core.exceptions import AdapterError


def test_load_csv(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "test.csv"
    sample_df.to_csv(path, index=False)
    result = load_dataframe(path)
    assert len(result) == len(sample_df)
    assert list(result.columns) == list(sample_df.columns)


def test_load_nonexistent_file() -> None:
    with pytest.raises(AdapterError, match="File not found"):
        load_dataframe("/nonexistent/file.csv")


def test_load_unsupported_format(tmp_path: Path) -> None:
    path = tmp_path / "test.xyz"
    path.write_text("data")
    with pytest.raises(AdapterError, match="Unsupported format"):
        load_dataframe(path)


def test_load_json(tmp_path: Path, sample_df: pd.DataFrame) -> None:
    path = tmp_path / "test.json"
    sample_df.to_json(path, orient="records")
    result = load_dataframe(path)
    assert len(result) == len(sample_df)
