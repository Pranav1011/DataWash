"""Detect similar/potentially duplicate columns using string similarity."""

from __future__ import annotations

from difflib import SequenceMatcher
from itertools import combinations

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class SimilarityDetector(BaseDetector):
    def __init__(
        self, name_threshold: float = 0.8, value_threshold: float = 0.7
    ) -> None:
        self._name_threshold = name_threshold
        self._value_threshold = value_threshold

    @property
    def name(self) -> str:
        return "similarity"

    @property
    def description(self) -> str:
        return "Detects similar or potentially duplicate columns"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []
        columns = list(df.columns)
        if len(columns) < 2:
            return findings

        for col_a, col_b in combinations(columns, 2):
            name_sim = SequenceMatcher(None, col_a.lower(), col_b.lower()).ratio()
            if name_sim < self._name_threshold * 0.5:
                continue  # Skip if names are very different

            value_sim = self._value_similarity(df[col_a], df[col_b])
            combined = 0.4 * name_sim + 0.6 * value_sim

            if combined > self._value_threshold:
                findings.append(
                    Finding(
                        detector=self.name,
                        issue_type="similar_columns",
                        severity=Severity.MEDIUM,
                        columns=[col_a, col_b],
                        details={
                            "name_similarity": round(name_sim, 3),
                            "value_similarity": round(value_sim, 3),
                            "combined_score": round(combined, 3),
                        },
                        message=(
                            f"Columns '{col_a}' and '{col_b}' "
                            f"appear similar (score: {combined:.2f})"
                        ),
                        confidence=combined,
                    )
                )
        return findings

    def _value_similarity(self, a: pd.Series, b: pd.Series) -> float:
        """Compute value overlap between two columns."""
        a_set = set(a.dropna().astype(str).unique())
        b_set = set(b.dropna().astype(str).unique())
        if not a_set and not b_set:
            return 0.0
        intersection = len(a_set & b_set)
        union = len(a_set | b_set)
        return intersection / union if union > 0 else 0.0


register_detector(SimilarityDetector())
