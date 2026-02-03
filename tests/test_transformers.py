"""Tests for transformers."""
from __future__ import annotations
import numpy as np
import pandas as pd
import pytest
from datawash.transformers import run_transformer


class TestDuplicateTransformer:
    def test_removes_duplicates(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 2, 3], "b": ["x", "y", "y", "z"]})
        result_df, result = run_transformer("duplicates", df, keep="first")
        assert len(result_df) == 3
        assert result.rows_affected == 1

    def test_no_duplicates(self, sample_df: pd.DataFrame) -> None:
        result_df, result = run_transformer("duplicates", sample_df)
        assert len(result_df) == len(sample_df)
        assert result.rows_affected == 0


class TestMissingTransformer:
    def test_drop_rows(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", "z"]})
        result_df, result = run_transformer("missing", df, strategy="drop_rows", columns=["a"])
        assert len(result_df) == 2

    def test_fill_median(self) -> None:
        df = pd.DataFrame({"a": [1.0, None, 3.0, 5.0]})
        result_df, result = run_transformer("missing", df, strategy="fill_median", columns=["a"])
        assert result_df["a"].isna().sum() == 0
        assert result_df["a"].iloc[1] == 3.0  # median of 1, 3, 5

    def test_fill_value(self) -> None:
        df = pd.DataFrame({"a": ["x", None, "z"]})
        result_df, result = run_transformer("missing", df, strategy="fill_value", columns=["a"], fill_value="MISSING")
        assert result_df["a"].iloc[1] == "MISSING"

    def test_empty_to_nan(self) -> None:
        df = pd.DataFrame({"a": ["x", "", "z"]})
        result_df, result = run_transformer("missing", df, strategy="empty_to_nan", columns=["a"])
        assert pd.isna(result_df["a"].iloc[1])


class TestTypeTransformer:
    def test_to_numeric(self) -> None:
        df = pd.DataFrame({"a": ["1", "2", "3"]})
        result_df, result = run_transformer("types", df, columns=["a"], target_type="numeric")
        assert pd.api.types.is_numeric_dtype(result_df["a"])

    def test_to_boolean(self) -> None:
        df = pd.DataFrame({"a": ["yes", "no", "yes"]})
        result_df, result = run_transformer("types", df, columns=["a"], target_type="boolean")
        assert result_df["a"].iloc[0] == True
        assert result_df["a"].iloc[1] == False


class TestFormatTransformer:
    def test_strip_whitespace(self) -> None:
        df = pd.DataFrame({"a": [" hello ", "world ", " foo"]})
        result_df, result = run_transformer("formats", df, columns=["a"], operation="strip_whitespace")
        assert result_df["a"].tolist() == ["hello", "world", "foo"]

    def test_lowercase(self) -> None:
        df = pd.DataFrame({"a": ["Hello", "WORLD", "Foo"]})
        result_df, result = run_transformer("formats", df, columns=["a"], operation="lowercase")
        assert result_df["a"].tolist() == ["hello", "world", "foo"]


class TestColumnTransformer:
    def test_drop_columns(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result_df, result = run_transformer("columns", df, columns=["b"], operation="drop")
        assert "b" not in result_df.columns
        assert "a" in result_df.columns

    def test_rename_columns(self) -> None:
        df = pd.DataFrame({"old_name": [1]})
        result_df, result = run_transformer("columns", df, operation="rename", mapping={"old_name": "new_name"})
        assert "new_name" in result_df.columns


class TestCategoryTransformer:
    def test_normalize(self) -> None:
        df = pd.DataFrame({"a": [" Cat ", "DOG", "cat"]})
        result_df, result = run_transformer("categories", df, columns=["a"])
        assert result_df["a"].tolist() == ["cat", "dog", "cat"]

    def test_mapping(self) -> None:
        df = pd.DataFrame({"a": ["US", "USA", "United States"]})
        result_df, result = run_transformer("categories", df, columns=["a"], mapping={"USA": "US", "United States": "US"})
        assert result_df["a"].tolist() == ["US", "US", "US"]
