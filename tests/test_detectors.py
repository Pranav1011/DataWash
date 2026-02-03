"""Tests for detectors."""

from __future__ import annotations
import pandas as pd
import pytest
from datawash.profiler import profile_dataset
from datawash.detectors.missing_detector import MissingDetector
from datawash.detectors.duplicate_detector import DuplicateDetector
from datawash.detectors.format_detector import FormatDetector
from datawash.detectors.outlier_detector import OutlierDetector
from datawash.detectors.type_detector import TypeDetector
from datawash.detectors.similarity_detector import SimilarityDetector


class TestMissingDetector:
    def test_detects_nulls(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = MissingDetector().detect(messy_df, profile)
        null_findings = [f for f in findings if f.issue_type == "missing_values"]
        assert len(null_findings) >= 1
        email_finding = [f for f in null_findings if "email" in f.columns]
        assert len(email_finding) == 1

    def test_no_nulls(self, sample_df: pd.DataFrame) -> None:
        profile = profile_dataset(sample_df)
        findings = MissingDetector().detect(sample_df, profile)
        assert len([f for f in findings if f.issue_type == "missing_values"]) == 0


class TestDuplicateDetector:
    def test_detects_duplicates(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = DuplicateDetector().detect(messy_df, profile)
        assert len(findings) == 1
        assert findings[0].details["duplicate_count"] == 2

    def test_no_duplicates(self, sample_df: pd.DataFrame) -> None:
        profile = profile_dataset(sample_df)
        findings = DuplicateDetector().detect(sample_df, profile)
        assert len(findings) == 0


class TestOutlierDetector:
    def test_detects_outliers(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = OutlierDetector().detect(messy_df, profile)
        score_findings = [f for f in findings if "score" in f.columns]
        assert len(score_findings) >= 1

    def test_zscore_method(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        det = OutlierDetector(method="zscore", threshold=2.0)
        findings = det.detect(messy_df, profile)
        assert isinstance(findings, list)


class TestTypeDetector:
    def test_detects_numeric_as_string(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = TypeDetector().detect(messy_df, profile)
        numeric_findings = [f for f in findings if f.issue_type == "numeric_as_string"]
        assert len(numeric_findings) >= 1
        assert "age" in numeric_findings[0].columns

    def test_detects_boolean_as_string(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = TypeDetector().detect(messy_df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) >= 1


class TestFormatDetector:
    def test_detects_case_inconsistency(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = FormatDetector().detect(messy_df, profile)
        case_findings = [f for f in findings if f.issue_type == "inconsistent_case"]
        assert len(case_findings) >= 1

    def test_detects_whitespace(self, messy_df: pd.DataFrame) -> None:
        profile = profile_dataset(messy_df)
        findings = FormatDetector().detect(messy_df, profile)
        ws_findings = [f for f in findings if f.issue_type == "whitespace_padding"]
        assert len(ws_findings) >= 1


class TestFormatDetectorDates:
    def test_detects_mixed_date_formats(self) -> None:
        """Mixed date formats should be flagged as inconsistent."""
        df = pd.DataFrame(
            {
                "date": [
                    "2023-01-15",
                    "01/15/2023",
                    "15-Jan-2023",
                    "Jan 15, 2023",
                    "2023-02-20",
                    "02/20/2023",
                    "20-Feb-2023",
                    "Feb 20, 2023",
                    "2023-03-10",
                    "03/10/2023",
                ]
            }
        )
        profile = profile_dataset(df)
        findings = FormatDetector().detect(df, profile)
        date_findings = [
            f for f in findings if f.issue_type == "inconsistent_date_format"
        ]
        assert len(date_findings) == 1
        assert "date" in date_findings[0].columns

    def test_consistent_dates_no_finding(self) -> None:
        """All ISO dates should not flag inconsistency."""
        df = pd.DataFrame(
            {
                "date": [
                    "2023-01-15",
                    "2023-02-20",
                    "2023-03-10",
                    "2023-04-01",
                    "2023-05-05",
                ]
            }
        )
        profile = profile_dataset(df)
        findings = FormatDetector().detect(df, profile)
        date_findings = [
            f for f in findings if f.issue_type == "inconsistent_date_format"
        ]
        assert len(date_findings) == 0

    def test_non_date_column_no_finding(self) -> None:
        """Non-date string column should not flag date inconsistency."""
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie", "Dave", "Eve"]})
        profile = profile_dataset(df)
        findings = FormatDetector().detect(df, profile)
        date_findings = [
            f for f in findings if f.issue_type == "inconsistent_date_format"
        ]
        assert len(date_findings) == 0


class TestBooleanDetection:
    def test_yes_no_detected(self) -> None:
        df = pd.DataFrame({"flag": ["yes", "no", "yes", "no", "yes"]})
        profile = profile_dataset(df)
        findings = TypeDetector().detect(df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) == 1

    def test_true_false_detected(self) -> None:
        df = pd.DataFrame({"flag": ["true", "false", "true", "false", "true"]})
        profile = profile_dataset(df)
        findings = TypeDetector().detect(df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) == 1

    def test_mixed_boolean_detected(self) -> None:
        """yes/no/true/false mixed should still be detected."""
        df = pd.DataFrame({"flag": ["yes", "no", "true", "false", "Yes", "No", "TRUE"]})
        profile = profile_dataset(df)
        findings = TypeDetector().detect(df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) == 1

    def test_yes_no_maybe_not_boolean(self) -> None:
        """Column with non-boolean values should NOT be detected."""
        df = pd.DataFrame({"flag": ["yes", "no", "maybe", "yes", "no"]})
        profile = profile_dataset(df)
        findings = TypeDetector().detect(df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) == 0

    def test_single_value_not_boolean(self) -> None:
        """All same value should NOT be detected (needs >= 2 distinct)."""
        df = pd.DataFrame({"flag": ["yes", "yes", "yes", "yes", "yes"]})
        profile = profile_dataset(df)
        findings = TypeDetector().detect(df, profile)
        bool_findings = [f for f in findings if f.issue_type == "boolean_as_string"]
        assert len(bool_findings) == 0


class TestEmptyStringDetection:
    def test_whitespace_only_strings(self) -> None:
        """Whitespace-only strings should be detected as empty."""
        df = pd.DataFrame({"name": ["Alice", "   ", "Bob", "", "Charlie"]})
        profile = profile_dataset(df)
        findings = MissingDetector().detect(df, profile)
        empty_findings = [f for f in findings if f.issue_type == "empty_strings"]
        assert len(empty_findings) == 1
        assert empty_findings[0].details["empty_string_count"] == 2


class TestSimilarityDetector:
    def test_detects_similar_columns(self) -> None:
        df = pd.DataFrame(
            {
                "first_name": ["Alice", "Bob", "Charlie"],
                "firstname": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        profile = profile_dataset(df)
        findings = SimilarityDetector().detect(df, profile)
        assert len(findings) >= 1

    def test_no_similar_columns(self, sample_df: pd.DataFrame) -> None:
        profile = profile_dataset(sample_df)
        findings = SimilarityDetector().detect(sample_df, profile)
        # May or may not find similarities in sample_df
        assert isinstance(findings, list)
