"""Detect duplicate rows."""

from __future__ import annotations

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class DuplicateDetector(BaseDetector):
    @property
    def name(self) -> str:
        return "duplicates"

    @property
    def description(self) -> str:
        return "Detects exact duplicate rows"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []
        dup_count = profile.duplicate_row_count
        if dup_count == 0:
            return findings

        ratio = dup_count / profile.row_count if profile.row_count > 0 else 0
        if ratio > 0.1:
            severity = Severity.HIGH
        elif ratio > 0.01:
            severity = Severity.MEDIUM
        else:
            severity = Severity.LOW

        dup_mask = df.duplicated(keep="first")
        dup_indices = df.index[dup_mask].tolist()[:100]  # Cap for memory

        findings.append(
            Finding(
                detector=self.name,
                issue_type="duplicate_rows",
                severity=severity,
                columns=list(df.columns),
                rows=dup_indices,
                details={
                    "duplicate_count": dup_count,
                    "duplicate_ratio": round(ratio, 4),
                },
                message=f"Found {dup_count} exact duplicate rows ({ratio:.1%} of data)",
                confidence=1.0,
            )
        )
        return findings


register_detector(DuplicateDetector())
