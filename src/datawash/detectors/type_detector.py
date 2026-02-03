"""Semantic type detection using pattern matching."""

from __future__ import annotations

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class TypeDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "types"

    @property
    def description(self) -> str:
        return "Detects semantic types and type mismatches"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []

        for col_name, col_profile in profile.columns.items():
            is_string = pd.api.types.is_string_dtype(df[col_name])

            # Flag numeric columns stored as strings
            if is_string:
                series = df[col_name].dropna()
                if series.empty:
                    continue
                numeric_count = pd.to_numeric(series, errors="coerce").notna().sum()
                ratio = numeric_count / len(series)
                if ratio > 0.8:
                    findings.append(
                        Finding(
                            detector=self.name,
                            issue_type="numeric_as_string",
                            severity=Severity.MEDIUM,
                            columns=[col_name],
                            details={"numeric_ratio": round(float(ratio), 3)},
                            message=(
                                f"Column '{col_name}' appears numeric "
                                f"but stored as string "
                                f"({ratio:.0%} parseable)"
                            ),
                            confidence=float(ratio),
                        )
                    )

            # Flag boolean-like columns
            if is_string:
                bool_values = {
                    "true",
                    "false",
                    "yes",
                    "no",
                    "y",
                    "n",
                    "1",
                    "0",
                    "t",
                    "f",
                    "on",
                    "off",
                }
                lowered_unique = set(
                    df[col_name].dropna().astype(str).str.strip().str.lower().unique()
                )
                if lowered_unique <= bool_values and len(lowered_unique) >= 2:
                    findings.append(
                        Finding(
                            detector=self.name,
                            issue_type="boolean_as_string",
                            severity=Severity.LOW,
                            columns=[col_name],
                            details={"values": sorted(lowered_unique)},
                            message=(
                                f"Column '{col_name}' contains "
                                f"boolean-like values "
                                f"stored as strings"
                            ),
                            confidence=0.95,
                        )
                    )

            # Report detected semantic types from patterns
            if col_profile.patterns:
                for pattern_name, pattern_info in col_profile.patterns.items():
                    col_profile.semantic_type = pattern_name

        return findings


register_detector(TypeDetector())
