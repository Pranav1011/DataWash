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
        result_df, result = run_transformer(
            "missing", df, strategy="drop_rows", columns=["a"]
        )
        assert len(result_df) == 2

    def test_fill_median(self) -> None:
        df = pd.DataFrame({"a": [1.0, None, 3.0, 5.0]})
        result_df, result = run_transformer(
            "missing", df, strategy="fill_median", columns=["a"]
        )
        assert result_df["a"].isna().sum() == 0
        assert result_df["a"].iloc[1] == 3.0  # median of 1, 3, 5

    def test_fill_value(self) -> None:
        df = pd.DataFrame({"a": ["x", None, "z"]})
        result_df, result = run_transformer(
            "missing", df, strategy="fill_value", columns=["a"], fill_value="MISSING"
        )
        assert result_df["a"].iloc[1] == "MISSING"

    def test_empty_to_nan(self) -> None:
        df = pd.DataFrame({"a": ["x", "", "z"]})
        result_df, result = run_transformer(
            "missing", df, strategy="empty_to_nan", columns=["a"]
        )
        assert pd.isna(result_df["a"].iloc[1])

    def test_clean_empty_strings_fills_with_mode(self) -> None:
        """clean_empty_strings converts empty/whitespace to NaN and fills in one step."""
        df = pd.DataFrame({"a": ["x", "", "x", "  ", "y"]})
        result_df, result = run_transformer(
            "missing", df, strategy="clean_empty_strings", columns=["a"]
        )
        # No NaN values should remain
        assert result_df["a"].isna().sum() == 0
        # Empty strings should be filled with mode ("x")
        assert result_df["a"].iloc[1] == "x"
        assert result_df["a"].iloc[3] == "x"

    def test_clean_empty_strings_uses_specified_fill_value(self) -> None:
        """clean_empty_strings can use a custom fill value."""
        df = pd.DataFrame({"a": ["x", "", "y"]})
        result_df, result = run_transformer(
            "missing",
            df,
            strategy="clean_empty_strings",
            columns=["a"],
            fill_strategy="value",
            fill_value="UNKNOWN",
        )
        assert result_df["a"].iloc[1] == "UNKNOWN"


class TestTypeTransformer:
    def test_to_numeric(self) -> None:
        df = pd.DataFrame({"a": ["1", "2", "3"]})
        result_df, result = run_transformer(
            "types", df, columns=["a"], target_type="numeric"
        )
        assert pd.api.types.is_numeric_dtype(result_df["a"])

    def test_to_boolean(self) -> None:
        df = pd.DataFrame({"a": ["yes", "no", "yes"]})
        result_df, result = run_transformer(
            "types", df, columns=["a"], target_type="boolean"
        )
        assert result_df["a"].iloc[0] == True
        assert result_df["a"].iloc[1] == False


class TestFormatTransformer:
    def test_strip_whitespace(self) -> None:
        df = pd.DataFrame({"a": [" hello ", "world ", " foo"]})
        result_df, result = run_transformer(
            "formats", df, columns=["a"], operation="strip_whitespace"
        )
        assert result_df["a"].tolist() == ["hello", "world", "foo"]

    def test_lowercase(self) -> None:
        df = pd.DataFrame({"a": ["Hello", "WORLD", "Foo"]})
        result_df, result = run_transformer(
            "formats", df, columns=["a"], operation="lowercase"
        )
        assert result_df["a"].tolist() == ["hello", "world", "foo"]


class TestColumnTransformer:
    def test_drop_columns(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        result_df, result = run_transformer(
            "columns", df, columns=["b"], operation="drop"
        )
        assert "b" not in result_df.columns
        assert "a" in result_df.columns

    def test_rename_columns(self) -> None:
        df = pd.DataFrame({"old_name": [1]})
        result_df, result = run_transformer(
            "columns", df, operation="rename", mapping={"old_name": "new_name"}
        )
        assert "new_name" in result_df.columns


class TestCategoryTransformer:
    def test_normalize(self) -> None:
        df = pd.DataFrame({"a": [" Cat ", "DOG", "cat"]})
        result_df, result = run_transformer("categories", df, columns=["a"])
        assert result_df["a"].tolist() == ["cat", "dog", "cat"]

    def test_mapping(self) -> None:
        df = pd.DataFrame({"a": ["US", "USA", "United States"]})
        result_df, result = run_transformer(
            "categories",
            df,
            columns=["a"],
            mapping={"USA": "US", "United States": "US"},
        )
        assert result_df["a"].tolist() == ["US", "US", "US"]


class TestStandardizeDates:
    def _mixed(self) -> pd.DataFrame:
        return pd.DataFrame(
            {"d": ["2024-01-15", "01/20/2024", "Jan 30, 2024", None, "not a date"]}
        )

    def test_mixed_formats_are_standardized(self) -> None:
        result_df, result = run_transformer(
            "formats", self._mixed(), columns=["d"], operation="standardize_dates"
        )
        assert result_df["d"].tolist()[:3] == ["2024-01-15", "2024-01-20", "2024-01-30"]
        assert pd.isna(result_df["d"].iloc[3])
        assert result_df["d"].iloc[4] == "not a date"
        assert result.rows_affected == 2

    def test_generated_code_reproduces_transform(self) -> None:
        df = self._mixed()
        result_df, result = run_transformer(
            "formats", df, columns=["d"], operation="standardize_dates"
        )
        namespace = {"pd": pd, "df": df.copy()}
        exec(result.code, namespace)
        pd.testing.assert_frame_equal(namespace["df"], result_df)

    def test_types_datetime_parses_mixed_formats(self) -> None:
        df = pd.DataFrame({"d": ["2024-01-15", "01/20/2024", "Jan 30, 2024"]})
        result_df, result = run_transformer(
            "types", df, columns=["d"], target_type="datetime"
        )
        assert result_df["d"].notna().all()
        namespace = {"pd": pd, "df": df.copy()}
        exec(result.code, namespace)
        pd.testing.assert_frame_equal(namespace["df"], result_df)


DATE_ORDER_CASES = {
    "day_first": (
        ["13/04/2024", "03/04/2024", "25/12/2023"],
        ["2024-04-13", "2024-04-03", "2023-12-25"],
        0,
    ),
    "month_first": (
        ["04/13/2024", "03/04/2024", "2024-01-05"],
        ["2024-04-13", "2024-03-04", "2024-01-05"],
        0,
    ),
    "mixed_evidence": (
        ["13/04/2024", "01/20/2024", "03/04/2024", "05/05/2024"],
        ["2024-04-13", "2024-01-20", "03/04/2024", "2024-05-05"],
        1,
    ),
    "all_ambiguous": (
        ["03/04/2024", "2024-01-05", "Jan 30, 2024"],
        ["03/04/2024", "2024-01-05", "2024-01-30"],
        1,
    ),
    "demo_csv": (
        ["2024-01-15", "01/20/2024", "2024-01-25", "2024-01-15", "Jan 30, 2024"],
        ["2024-01-15", "2024-01-20", "2024-01-25", "2024-01-15", "2024-01-30"],
        0,
    ),
}


@pytest.mark.parametrize("case", DATE_ORDER_CASES)
def test_standardize_dates_day_month_order(case: str) -> None:
    values, expected, n_ambiguous = DATE_ORDER_CASES[case]
    df = pd.DataFrame({"d": values})
    result_df, result = run_transformer(
        "formats", df, columns=["d"], operation="standardize_dates"
    )
    assert result_df["d"].tolist() == expected
    changed = sum(v != e for v, e in zip(values, expected))
    assert result.rows_affected == changed
    if n_ambiguous:
        assert result.warnings == [
            f"{n_ambiguous} ambiguous date(s) in 'd' left unchanged: "
            "could be day-first or month-first"
        ]
    else:
        assert result.warnings == []

    namespace = {"pd": pd, "df": df.copy()}
    exec(result.code, namespace)
    pd.testing.assert_frame_equal(namespace["df"], result_df)


@pytest.mark.parametrize("case", DATE_ORDER_CASES)
def test_types_datetime_day_month_order(case: str) -> None:
    values, expected, n_ambiguous = DATE_ORDER_CASES[case]
    df = pd.DataFrame({"d": values})
    result_df, result = run_transformer(
        "types", df, columns=["d"], target_type="datetime"
    )
    for got, want in zip(result_df["d"], expected):
        if want[:4].isdigit() and want[4] == "-":
            assert got.strftime("%Y-%m-%d") == want
        else:
            assert pd.isna(got)
    assert len(result.warnings) == (1 if n_ambiguous else 0)

    namespace = {"pd": pd, "df": df.copy()}
    exec(result.code, namespace)
    pd.testing.assert_frame_equal(namespace["df"], result_df)
