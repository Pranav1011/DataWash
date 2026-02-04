"""Tests for SmartSampler."""

import numpy as np
import pandas as pd
import pytest

from datawash.core.sampling import SAMPLE_SIZE, SAMPLE_THRESHOLD, SmartSampler


class TestSmartSampler:
    def test_no_sampling_below_threshold(self):
        df = pd.DataFrame({"a": range(SAMPLE_THRESHOLD - 1000)})
        sampler = SmartSampler(df)
        assert not sampler.is_sampled
        assert len(sampler.sample_df) == len(df)
        assert sampler.scale_factor == 1.0

    def test_sampling_above_threshold(self):
        n = SAMPLE_THRESHOLD + 10_000
        df = pd.DataFrame({"a": range(n)})
        sampler = SmartSampler(df)
        assert sampler.is_sampled
        assert len(sampler.sample_df) <= SAMPLE_SIZE + 2000  # Allow buffer

    def test_preserves_null_rows(self):
        n = SAMPLE_THRESHOLD + 10_000
        data = {"a": [None if i < 200 else i for i in range(n)], "b": range(n)}
        df = pd.DataFrame(data)
        sampler = SmartSampler(df)
        null_count = sampler.sample_df["a"].isna().sum()
        assert null_count > 0

    def test_scale_factor_correct(self):
        n = 100_000
        df = pd.DataFrame({"a": range(n)})
        sampler = SmartSampler(df)
        expected_factor = n / len(sampler.sample_df)
        assert abs(sampler.scale_factor - expected_factor) < 0.01

    def test_extrapolate_count(self):
        df = pd.DataFrame({"a": range(100_000)})
        sampler = SmartSampler(df)
        extrapolated = sampler.extrapolate_count(100)
        assert extrapolated > 100  # Should be scaled up

    def test_extrapolate_no_sampling(self):
        df = pd.DataFrame({"a": range(1000)})
        sampler = SmartSampler(df)
        assert sampler.extrapolate_count(50) == 50

    def test_stratified_sampling_preserves_categories(self):
        n = SAMPLE_THRESHOLD + 10_000
        df = pd.DataFrame(
            {
                "category": (["A"] * (n // 2)) + (["B"] * (n - n // 2)),
                "value": range(n),
            }
        )
        sampler = SmartSampler(df)
        categories = sampler.sample_df["category"].unique()
        assert "A" in categories
        assert "B" in categories

    def test_sample_is_smaller_than_original(self):
        n = 200_000
        df = pd.DataFrame({"a": range(n), "b": range(n)})
        sampler = SmartSampler(df)
        assert len(sampler.sample_df) < len(df)
