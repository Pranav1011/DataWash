"""Tests for parallel processing."""

import pandas as pd
import pytest

from datawash.core.cache import ComputationCache
from datawash.core.models import DatasetProfile
from datawash.detectors.registry import get_all_detectors
from datawash.profiler.engine import profile_dataset
from datawash.profiler.parallel import profile_dataset_parallel, run_detectors_parallel


@pytest.fixture
def test_df():
    return pd.DataFrame(
        {
            "a": range(1000),
            "b": ["x", "y", "z"] * 333 + ["x"],
            "c": [1.1, 2.2, None] * 333 + [1.1],
        }
    )


class TestParallelProfiler:
    def test_returns_dataset_profile(self, test_df):
        profile = profile_dataset_parallel(test_df)
        assert isinstance(profile, DatasetProfile)

    def test_profiles_all_columns(self, test_df):
        profile = profile_dataset_parallel(test_df)
        assert len(profile.columns) == 3
        assert "a" in profile.columns
        assert "b" in profile.columns
        assert "c" in profile.columns

    def test_profile_row_count(self, test_df):
        profile = profile_dataset_parallel(test_df)
        assert profile.row_count == 1000

    def test_profile_null_count(self, test_df):
        profile = profile_dataset_parallel(test_df)
        assert profile.columns["c"].null_count > 0
        assert profile.columns["a"].null_count == 0

    def test_with_cache(self, test_df):
        cache = ComputationCache(test_df)
        profile = profile_dataset_parallel(test_df, cache=cache)
        assert len(profile.columns) == 3

    def test_matches_sequential_profiler(self, test_df):
        parallel = profile_dataset_parallel(test_df)
        sequential = profile_dataset(test_df)
        assert parallel.row_count == sequential.row_count
        assert parallel.column_count == sequential.column_count
        for col_name in test_df.columns:
            assert (
                parallel.columns[col_name].null_count
                == sequential.columns[col_name].null_count
            )
            assert (
                parallel.columns[col_name].unique_count
                == sequential.columns[col_name].unique_count
            )


class TestParallelDetectors:
    def test_runs_all_detectors(self, test_df):
        profile = profile_dataset(test_df)
        detectors = get_all_detectors()
        findings = run_detectors_parallel(test_df, profile, detectors)
        assert isinstance(findings, list)

    def test_returns_findings(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 2, 3, 3],
                "b": [None, None, "x", "y", "z"],
            }
        )
        profile = profile_dataset(df)
        detectors = get_all_detectors()
        findings = run_detectors_parallel(df, profile, detectors)
        assert len(findings) > 0

    def test_matches_sequential_detectors(self, test_df):
        from datawash.detectors import run_all_detectors

        profile = profile_dataset(test_df)
        detectors = get_all_detectors()

        parallel_findings = run_detectors_parallel(test_df, profile, detectors)
        sequential_findings = run_all_detectors(test_df, profile)

        parallel_types = sorted({f.issue_type for f in parallel_findings})
        sequential_types = sorted({f.issue_type for f in sequential_findings})
        assert parallel_types == sequential_types
