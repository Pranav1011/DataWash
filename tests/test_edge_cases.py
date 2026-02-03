"""Edge case and coverage-boosting tests."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from datawash import analyze
from datawash.core.config import Config
from datawash.core.exceptions import AdapterError, DataWashError
from datawash.core.models import Severity
from datawash.profiler import profile_dataset
from datawash.profiler.patterns import detect_column_patterns
from datawash.profiler.statistics import (
    compute_categorical_stats,
    compute_numeric_stats,
)
from datawash.detectors.format_detector import FormatDetector
from datawash.detectors.outlier_detector import OutlierDetector
from datawash.detectors.registry import get_all_detectors, run_all_detectors
from datawash.suggestors.engine import generate_suggestions
from datawash.transformers import run_transformer
from datawash.transformers.registry import get_transformer
from datawash.codegen import generate_code
from datawash.core.models import TransformationResult


class TestConfigEdgeCases:
    def test_from_dict(self) -> None:
        config = Config.from_dict({"sample_size": 500})
        assert config.sample_size == 500

    def test_default_config(self) -> None:
        config = Config()
        assert config.sample_size == 10000
        assert config.use_case == "general"


class TestProfilerEdgeCases:
    def test_all_nulls_column(self) -> None:
        df = pd.DataFrame({"a": [None, None, None]})
        profile = profile_dataset(df)
        assert profile.columns["a"].null_count == 3

    def test_numeric_stats_empty(self) -> None:
        result = compute_numeric_stats(pd.Series([], dtype=float))
        assert result == {}

    def test_categorical_stats_empty(self) -> None:
        result = compute_categorical_stats(pd.Series([], dtype=str))
        assert result == {}

    def test_pattern_detection_email(self) -> None:
        series = pd.Series(["a@b.com", "c@d.org", "e@f.net"] * 10)
        patterns = detect_column_patterns(series)
        assert "email" in patterns

    def test_pattern_detection_empty(self) -> None:
        series = pd.Series([], dtype=str)
        patterns = detect_column_patterns(series)
        assert patterns == {}

    def test_single_value_numeric_stats(self) -> None:
        result = compute_numeric_stats(pd.Series([42.0]))
        assert result["mean"] == 42.0
        assert result["std"] == 0.0


class TestDetectorEdgeCases:
    def test_get_all_detectors(self) -> None:
        detectors = get_all_detectors()
        assert "missing" in detectors
        assert "duplicates" in detectors

    def test_run_with_enabled_filter(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3]})
        profile = profile_dataset(df)
        findings = run_all_detectors(df, profile, enabled=["missing"])
        detectors_used = {f.detector for f in findings}
        assert detectors_used <= {"missing"}

    def test_outlier_zero_iqr(self) -> None:
        df = pd.DataFrame({"a": [5.0] * 20})
        profile = profile_dataset(df)
        findings = OutlierDetector().detect(df, profile)
        assert len(findings) == 0

    def test_outlier_zscore_zero_std(self) -> None:
        df = pd.DataFrame({"a": [5.0] * 20})
        profile = profile_dataset(df)
        findings = OutlierDetector(method="zscore", threshold=2.0).detect(df, profile)
        assert len(findings) == 0

    def test_format_detector_short_column(self) -> None:
        df = pd.DataFrame({"a": ["hi", "lo"]})
        profile = profile_dataset(df)
        findings = FormatDetector().detect(df, profile)
        assert isinstance(findings, list)

    def test_duplicate_no_duplicates(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        profile = profile_dataset(df)
        from datawash.detectors.duplicate_detector import DuplicateDetector

        findings = DuplicateDetector().detect(df, profile)
        assert len(findings) == 0


class TestTransformerEdgeCases:
    def test_unknown_transformer(self) -> None:
        with pytest.raises(KeyError, match="Unknown transformer"):
            get_transformer("nonexistent")

    def test_missing_fill_mode(self) -> None:
        df = pd.DataFrame({"a": ["x", "x", None, "y"]})
        result_df, result = run_transformer(
            "missing", df, strategy="fill_mode", columns=["a"]
        )
        assert result_df["a"].isna().sum() == 0

    def test_fill_mode_all_null_leaves_unchanged(self) -> None:
        """fill_mode on all-null column should leave data unchanged, not drop rows."""
        df = pd.DataFrame({"a": [None, None, None]})
        result_df, result = run_transformer(
            "missing", df, strategy="fill_mode", columns=["a"]
        )
        # Should still have 3 rows (not dropped)
        assert len(result_df) == 3
        assert result.rows_affected == 0

    def test_formats_uppercase(self) -> None:
        df = pd.DataFrame({"a": ["hello", "world"]})
        result_df, _ = run_transformer(
            "formats", df, columns=["a"], operation="uppercase"
        )
        assert result_df["a"].tolist() == ["HELLO", "WORLD"]

    def test_formats_titlecase(self) -> None:
        df = pd.DataFrame({"a": ["hello world", "foo bar"]})
        result_df, _ = run_transformer(
            "formats", df, columns=["a"], operation="titlecase"
        )
        assert result_df["a"].tolist() == ["Hello World", "Foo Bar"]

    def test_formats_standardize_dates(self) -> None:
        df = pd.DataFrame({"a": ["01/15/2024", "2024-02-20"]})
        result_df, _ = run_transformer(
            "formats",
            df,
            columns=["a"],
            operation="standardize_dates",
            target_format="%Y-%m-%d",
        )
        assert isinstance(result_df, pd.DataFrame)

    def test_types_datetime(self) -> None:
        df = pd.DataFrame({"a": ["2024-01-01", "2024-06-15"]})
        result_df, _ = run_transformer(
            "types", df, columns=["a"], target_type="datetime"
        )
        assert pd.api.types.is_datetime64_any_dtype(result_df["a"])

    def test_types_string(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        result_df, _ = run_transformer("types", df, columns=["a"], target_type="string")
        assert result_df["a"].dtype == object or pd.api.types.is_string_dtype(
            result_df["a"]
        )

    def test_columns_merge(self) -> None:
        df = pd.DataFrame({"first": ["A", "B"], "last": ["X", "Y"]})
        result_df, _ = run_transformer(
            "columns",
            df,
            columns=["first", "last"],
            operation="merge",
            new_name="full",
            separator=" ",
        )
        assert "full" in result_df.columns
        assert result_df["full"].tolist() == ["A X", "B Y"]

    def test_missing_clip_iqr(self) -> None:
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 100.0]})
        result_df, result = run_transformer(
            "missing",
            df,
            strategy="clip_outliers",
            columns=["a"],
            method="iqr",
            threshold=1.5,
        )
        assert result_df["a"].max() < 100.0

    def test_missing_clip_zscore(self) -> None:
        df = pd.DataFrame({"a": list(range(20)) + [1000.0]})
        result_df, result = run_transformer(
            "missing",
            df,
            strategy="clip_outliers",
            columns=["a"],
            method="zscore",
            threshold=2.0,
        )
        assert result_df["a"].max() < 1000.0

    def test_columns_nonexistent(self) -> None:
        df = pd.DataFrame({"a": [1]})
        result_df, _ = run_transformer(
            "types", df, columns=["nonexistent"], target_type="numeric"
        )
        assert list(result_df.columns) == ["a"]


class TestCodegenEdgeCases:
    def test_script_style(self) -> None:
        results = [
            TransformationResult(
                transformer="duplicates",
                params={"keep": "first"},
                rows_affected=1,
                columns_affected=["a"],
                code='df = df.drop_duplicates(keep="first")',
            )
        ]
        code = generate_code(results, style="script", include_comments=True)
        assert "read_csv" in code
        assert "to_csv" in code

    def test_no_comments(self) -> None:
        results = [
            TransformationResult(
                transformer="test",
                params={},
                rows_affected=0,
                columns_affected=[],
                code="df = df",
            )
        ]
        code = generate_code(results, style="function", include_comments=False)
        assert "# test:" not in code


class TestReportEdgeCases:
    def test_analyze_with_config_dict(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        report = analyze(df, config={"sample_size": 100})
        assert report.profile.row_count == 3

    def test_suggest_returns_list(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3]})
        report = analyze(df)
        suggestions = report.suggest()
        assert isinstance(suggestions, list)

    def test_apply_invalid_id(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3]})
        report = analyze(df)
        result = report.apply([999])
        assert isinstance(result, pd.DataFrame)

    def test_generate_code_auto_applies(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3]})
        report = analyze(df)
        code = report.generate_code()
        assert isinstance(code, str)
