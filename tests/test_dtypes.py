"""Tests for dtype optimization."""

import numpy as np
import pandas as pd
import pytest

from datawash.core.dtypes import optimize_dataframe


class TestOptimizeDataframe:
    def test_reduces_memory(self):
        df = pd.DataFrame(
            {
                "big_int": np.array([1, 2, 3, 4, 5], dtype="int64"),
                "big_float": np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype="float64"),
            }
        )
        original_mem = df.memory_usage(deep=True).sum()
        optimized = optimize_dataframe(df)
        optimized_mem = optimized.memory_usage(deep=True).sum()
        assert optimized_mem < original_mem

    def test_preserves_integer_values(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        optimized = optimize_dataframe(df)
        assert list(optimized["a"]) == [1, 2, 3, 4, 5]

    def test_preserves_float_values(self):
        df = pd.DataFrame({"a": [1.1, 2.2, 3.3, 4.4, 5.5]})
        optimized = optimize_dataframe(df)
        assert all(
            abs(a - b) < 0.01 for a, b in zip(optimized["a"], [1.1, 2.2, 3.3, 4.4, 5.5])
        )

    def test_downcasts_integers(self):
        df = pd.DataFrame({"a": np.array([1, 2, 3], dtype="int64")})
        optimized = optimize_dataframe(df)
        assert optimized["a"].dtype.itemsize < 8

    def test_downcasts_floats(self):
        df = pd.DataFrame({"a": np.array([1.0, 2.0, 3.0], dtype="float64")})
        optimized = optimize_dataframe(df)
        assert optimized["a"].dtype.itemsize < 8

    def test_preserves_string_columns(self):
        df = pd.DataFrame({"a": ["x", "y", "z"]})
        optimized = optimize_dataframe(df)
        assert optimized["a"].dtype == df["a"].dtype

    def test_handles_nulls(self):
        df = pd.DataFrame(
            {"a": pd.array([1, None, 3], dtype="Int64"), "b": ["x", None, "y"]}
        )
        optimized = optimize_dataframe(df)
        assert optimized["b"].isna().sum() == 1

    def test_does_not_modify_original(self):
        df = pd.DataFrame({"a": np.array([1, 2, 3], dtype="int64")})
        original_dtype = df["a"].dtype
        _ = optimize_dataframe(df)
        assert df["a"].dtype == original_dtype

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        optimized = optimize_dataframe(df)
        assert len(optimized.columns) == 0
