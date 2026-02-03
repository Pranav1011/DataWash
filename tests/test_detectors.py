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


class TestSimilarityDetector:
    def test_detects_similar_columns(self) -> None:
        df = pd.DataFrame({
            "first_name": ["Alice", "Bob", "Charlie"],
            "firstname": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        })
        profile = profile_dataset(df)
        findings = SimilarityDetector().detect(df, profile)
        assert len(findings) >= 1

    def test_no_similar_columns(self, sample_df: pd.DataFrame) -> None:
        profile = profile_dataset(sample_df)
        findings = SimilarityDetector().detect(sample_df, profile)
        # May or may not find similarities in sample_df
        assert isinstance(findings, list)
