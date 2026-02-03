"""Detect statistical outliers in numeric columns."""

from __future__ import annotations

import numpy as np
import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class OutlierDetector(BaseDetector):
    def __init__(self, method: str = "iqr", threshold: float = 1.5) -> None:
        self._method = method
        self._threshold = threshold

    @property
    def name(self) -> str:
        return "outliers"

    @property
    def description(self) -> str:
        return "Detects statistical outliers in numeric columns"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        findings: list[Finding] = []
        for col_name in df.select_dtypes(include=[np.number]).columns:
            series = df[col_name].dropna()
            if len(series) < 10:
                continue

            if self._method == "iqr":
                outlier_indices = self._iqr_outliers(series)
            else:
                outlier_indices = self._zscore_outliers(series)

            if len(outlier_indices) == 0:
                continue

            ratio = len(outlier_indices) / len(series)
            severity = (
                Severity.HIGH
                if ratio > 0.05
                else Severity.MEDIUM if ratio > 0.01 else Severity.LOW
            )

            findings.append(
                Finding(
                    detector=self.name,
                    issue_type="outliers",
                    severity=severity,
                    columns=[col_name],
                    rows=outlier_indices[:100],
                    details={
                        "outlier_count": len(outlier_indices),
                        "outlier_ratio": round(ratio, 4),
                        "method": self._method,
                        "threshold": self._threshold,
                    },
                    message=(
                        f"Column '{col_name}' has "
                        f"{len(outlier_indices)} outliers "
                        f"({ratio:.1%}) detected by "
                        f"{self._method.upper()}"
                    ),
                    confidence=0.85,
                )
            )
        return findings

    def _iqr_outliers(self, series: pd.Series) -> list[int]:
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return []
        lower = q1 - self._threshold * iqr
        upper = q3 + self._threshold * iqr
        mask = (series < lower) | (series > upper)
        return series.index[mask].tolist()

    def _zscore_outliers(self, series: pd.Series) -> list[int]:
        mean = series.mean()
        std = series.std()
        if std == 0:
            return []
        z_scores = ((series - mean) / std).abs()
        mask = z_scores > self._threshold
        return series.index[mask].tolist()


register_detector(OutlierDetector())
