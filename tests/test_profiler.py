"""Tests for the profiler."""
from __future__ import annotations
import pandas as pd
import pytest
from datawash.profiler import profile_dataset


def test_profile_basic(sample_df: pd.DataFrame) -> None:
    profile = profile_dataset(sample_df)
    assert profile.row_count == 10
    assert profile.column_count == 5
    assert "name" in profile.columns
    assert "age" in profile.columns
    assert profile.columns["age"].null_count == 0


def test_profile_with_nulls(messy_df: pd.DataFrame) -> None:
    profile = profile_dataset(messy_df)
    assert profile.columns["email"].null_count == 4
    assert profile.columns["email"].null_ratio == pytest.approx(0.4)


def test_profile_duplicates(messy_df: pd.DataFrame) -> None:
    profile = profile_dataset(messy_df)
    assert profile.duplicate_row_count == 2


def test_profile_numeric_stats(sample_df: pd.DataFrame) -> None:
    profile = profile_dataset(sample_df)
    stats = profile.columns["age"].statistics
    assert "mean" in stats
    assert "median" in stats
    assert stats["min"] == 25.0
    assert stats["max"] == 45.0


def test_profile_empty_df(empty_df: pd.DataFrame) -> None:
    profile = profile_dataset(empty_df)
    assert profile.row_count == 0
    assert profile.column_count == 0


def test_profile_single_row(single_row_df: pd.DataFrame) -> None:
    profile = profile_dataset(single_row_df)
    assert profile.row_count == 1
    assert profile.column_count == 2
