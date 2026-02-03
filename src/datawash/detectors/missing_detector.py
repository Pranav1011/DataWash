"""Detect missing value patterns."""

from __future__ import annotations

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class MissingDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "missing"

    @property
    def description(self) -> str:
        return "Detects missing values and null patterns"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []
        for col_name, col_profile in profile.columns.items():
            if col_profile.null_count == 0:
                continue
            ratio = col_profile.null_ratio
            if ratio > 0.5:
                severity = Severity.HIGH
            elif ratio > 0.1:
                severity = Severity.MEDIUM
            else:
                severity = Severity.LOW

            findings.append(
                Finding(
                    detector=self.name,
                    issue_type="missing_values",
                    severity=severity,
                    columns=[col_name],
                    details={
                        "null_count": col_profile.null_count,
                        "null_ratio": col_profile.null_ratio,
                    },
                    message=(
                        f"Column '{col_name}' has "
                        f"{col_profile.null_count} missing "
                        f"values ({col_profile.null_ratio:.1%})"
                    ),
                    confidence=1.0,
                )
            )

        # Detect columns that are entirely empty strings
        for col_name in df.columns:
            if pd.api.types.is_string_dtype(df[col_name]):
                empty_count = int((df[col_name] == "").sum())
                if empty_count > 0:
                    findings.append(
                        Finding(
                            detector=self.name,
                            issue_type="empty_strings",
                            severity=Severity.MEDIUM,
                            columns=[col_name],
                            details={"empty_string_count": empty_count},
                            message=(
                                f"Column '{col_name}' has "
                                f"{empty_count} empty strings "
                                f"that may represent missing values"
                            ),
                            confidence=0.9,
                        )
                    )
        return findings


register_detector(MissingDetector())
