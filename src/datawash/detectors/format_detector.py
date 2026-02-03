"""Detect inconsistent formats within columns."""

from __future__ import annotations

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class FormatDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "formats"

    @property
    def description(self) -> str:
        return "Detects inconsistent formats within columns"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []
        for col_name in df.columns:
            if not pd.api.types.is_string_dtype(df[col_name]):
                continue
            clean = df[col_name].dropna().astype(str)
            if clean.empty or len(clean) < 5:
                continue

            # Check for mixed case patterns
            case_finding = self._check_case_inconsistency(col_name, clean)
            if case_finding:
                findings.append(case_finding)

            # Check for mixed date formats
            if (
                col_name in profile.columns
                and "date" in profile.columns[col_name].patterns
            ):
                date_finding = self._check_date_formats(col_name, clean)
                if date_finding:
                    findings.append(date_finding)

            # Check for mixed whitespace/padding
            ws_finding = self._check_whitespace(col_name, clean)
            if ws_finding:
                findings.append(ws_finding)

        return findings

    def _check_case_inconsistency(
        self, col_name: str, series: pd.Series
    ) -> Finding | None:
        has_upper = series.str.isupper().any()
        has_lower = series.str.islower().any()
        has_title = series.str.istitle().any()
        case_types = sum([has_upper, has_lower, has_title])
        if case_types >= 2:
            return Finding(
                detector=self.name,
                issue_type="inconsistent_case",
                severity=Severity.LOW,
                columns=[col_name],
                details={
                    "has_upper": bool(has_upper),
                    "has_lower": bool(has_lower),
                    "has_title": bool(has_title),
                },
                message=(
                    f"Column '{col_name}' has inconsistent "
                    f"casing (mixed upper/lower/title case)"
                ),
                confidence=0.8,
            )
        return None

    def _check_date_formats(self, col_name: str, series: pd.Series) -> Finding | None:
        slash_dates = series.str.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$").sum()
        dash_dates = series.str.match(r"^\d{4}-\d{2}-\d{2}").sum()
        if slash_dates > 0 and dash_dates > 0:
            return Finding(
                detector=self.name,
                issue_type="inconsistent_date_format",
                severity=Severity.MEDIUM,
                columns=[col_name],
                details={
                    "slash_count": int(slash_dates),
                    "dash_count": int(dash_dates),
                },
                message=(
                    f"Column '{col_name}' has mixed date "
                    f"formats ({slash_dates} slash-style, "
                    f"{dash_dates} ISO-style)"
                ),
                confidence=0.85,
            )
        return None

    def _check_whitespace(self, col_name: str, series: pd.Series) -> Finding | None:
        leading = (series != series.str.lstrip()).sum()
        trailing = (series != series.str.rstrip()).sum()
        total = int(leading + trailing)
        if total > 0:
            return Finding(
                detector=self.name,
                issue_type="whitespace_padding",
                severity=Severity.LOW,
                columns=[col_name],
                details={
                    "leading_spaces": int(leading),
                    "trailing_spaces": int(trailing),
                },
                message=f"Column '{col_name}' has {total} values with leading/trailing whitespace",
                confidence=1.0,
            )
        return None


register_detector(FormatDetector())
