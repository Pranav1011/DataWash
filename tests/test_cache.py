"""Tests for ComputationCache."""

import numpy as np
import pandas as pd
import pytest

from datawash.core.cache import ComputationCache


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "int_col": [1, 2, 3, None, 5],
            "str_col": ["a", "b", "a", "c", None],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
    )


class TestComputationCache:
    def test_null_mask_cached(self, sample_df):
        cache = ComputationCache(sample_df)
        mask1 = cache.get_null_mask("int_col")
        mask2 = cache.get_null_mask("int_col")
        assert mask1 is mask2  # Same object

    def test_null_mask_correct(self, sample_df):
        cache = ComputationCache(sample_df)
        mask = cache.get_null_mask("int_col")
        assert mask.sum() == 1
        assert mask.iloc[3] == True  # noqa: E712

    def test_value_set_cached(self, sample_df):
        cache = ComputationCache(sample_df)
        set1 = cache.get_value_set("str_col")
        set2 = cache.get_value_set("str_col")
        assert set1 is set2

    def test_value_set_excludes_nulls(self, sample_df):
        cache = ComputationCache(sample_df)
        values = cache.get_value_set("str_col")
        assert "nan" not in values
        assert "None" not in values

    def test_value_set_max_values(self):
        df = pd.DataFrame({"col": [f"val_{i}" for i in range(500)]})
        cache = ComputationCache(df)
        values = cache.get_value_set("col", max_values=100)
        assert len(values) <= 100

    def test_unique_count_cached(self, sample_df):
        cache = ComputationCache(sample_df)
        count1 = cache.get_unique_count("str_col")
        count2 = cache.get_unique_count("str_col")
        assert count1 == count2 == 3  # a, b, c (null not counted by nunique)

    def test_statistics_numeric(self, sample_df):
        cache = ComputationCache(sample_df)
        stats = cache.get_statistics("float_col")
        assert "mean" in stats
        assert "std" in stats
        assert "min" in stats
        assert "max" in stats
        assert "q1" in stats
        assert "q3" in stats
        assert abs(stats["mean"] - 3.3) < 0.01

    def test_statistics_cached(self, sample_df):
        cache = ComputationCache(sample_df)
        stats1 = cache.get_statistics("float_col")
        stats2 = cache.get_statistics("float_col")
        assert stats1 is stats2

    def test_statistics_non_numeric(self, sample_df):
        cache = ComputationCache(sample_df)
        stats = cache.get_statistics("str_col")
        assert stats == {}

    def test_statistics_all_null(self):
        df = pd.DataFrame({"col": pd.array([None, None, None], dtype="float64")})
        cache = ComputationCache(df)
        stats = cache.get_statistics("col")
        assert stats == {}

    def test_independent_columns(self, sample_df):
        cache = ComputationCache(sample_df)
        mask_a = cache.get_null_mask("int_col")
        mask_b = cache.get_null_mask("str_col")
        assert mask_a is not mask_b
