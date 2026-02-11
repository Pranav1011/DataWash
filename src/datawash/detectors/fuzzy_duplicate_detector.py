"""Fuzzy (semantic) duplicate detector using ML embeddings.

Detects rows that are semantically similar but not exact duplicates.
This catches variations like:
- "John Smith" vs "Jon Smith" (typos)
- "123 Main St" vs "123 Main Street" (abbreviations)
- "NYC" vs "New York City" (synonyms)

Requires ML dependencies: pip install datawash[ml]
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector

logger = logging.getLogger(__name__)


class FuzzyDuplicateDetector(BaseDetector):
    """Detect semantically similar (fuzzy) duplicate rows using ML embeddings.

    This detector uses sentence-transformers to generate embeddings and
    find rows that are semantically similar but not exact duplicates.

    Requires: pip install datawash[ml]
    """

    # Configuration
    DEFAULT_THRESHOLD = 0.85
    MAX_ROWS_FOR_ML = 50_000  # Performance limit
    MIN_ROWS = 10  # Don't run on tiny datasets
    SAMPLE_SIZE = 10_000  # Sample size for large datasets

    def __init__(
        self,
        threshold: float = DEFAULT_THRESHOLD,
        columns: list[str] | None = None,
    ) -> None:
        """Initialize the fuzzy duplicate detector.

        Args:
            threshold: Similarity threshold (0-1). Default 0.85.
            columns: Columns to compare. If None, uses all string columns.
        """
        self._threshold = threshold
        self._columns = columns

    @property
    def name(self) -> str:
        return "fuzzy_duplicates"

    @property
    def description(self) -> str:
        return "Detects semantically similar (fuzzy) duplicate rows using ML"

    def detect(
        self, df: pd.DataFrame, profile: DatasetProfile, config: Any = None
    ) -> list[Finding]:
        """Detect fuzzy duplicates in the DataFrame.

        Args:
            df: Input DataFrame.
            profile: Dataset profile.
            config: Optional configuration.

        Returns:
            List of findings for fuzzy duplicate groups.
        """
        # Check if ML is available
        try:
            from datawash.ml import ML_AVAILABLE

            if not ML_AVAILABLE:
                logger.debug(
                    "ML dependencies not available, skipping fuzzy duplicate detection"
                )
                return []
        except ImportError:
            logger.debug("ML module not available, skipping fuzzy duplicate detection")
            return []

        # Skip small datasets
        if len(df) < self.MIN_ROWS:
            return []

        # Determine columns to analyze
        columns = self._columns
        if columns is None:
            # Use string/object columns
            columns = df.select_dtypes(include=["object", "string"]).columns.tolist()

        if not columns:
            return []

        # Validate columns exist
        columns = [c for c in columns if c in df.columns]
        if not columns:
            return []

        # Sample large datasets
        analysis_df = df
        if len(df) > self.MAX_ROWS_FOR_ML:
            logger.info(
                "Dataset too large (%d rows), sampling %d rows for fuzzy duplicate detection",
                len(df),
                self.SAMPLE_SIZE,
            )
            analysis_df = df.sample(n=self.SAMPLE_SIZE, random_state=42)

        try:
            return self._detect_fuzzy_duplicates(analysis_df, columns, len(df))
        except Exception as e:
            logger.warning("Fuzzy duplicate detection failed: %s", e)
            return []

    def _detect_fuzzy_duplicates(
        self,
        df: pd.DataFrame,
        columns: list[str],
        original_size: int,
    ) -> list[Finding]:
        """Run the fuzzy duplicate detection.

        Args:
            df: DataFrame to analyze (possibly sampled).
            columns: Columns to compare.
            original_size: Original dataset size (for extrapolation).

        Returns:
            List of findings.
        """
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        finder = FuzzyDuplicateFinder(model_preset="fast")
        groups = finder.find_duplicates(
            df,
            columns=columns,
            threshold=self._threshold,
        )

        if not groups:
            return []

        # Calculate total duplicate rows
        total_duplicates = sum(len(g) - 1 for g in groups)

        # Extrapolate if sampled
        if len(df) < original_size:
            scale_factor = original_size / len(df)
            estimated_duplicates = int(total_duplicates * scale_factor)
            estimated_groups = int(len(groups) * scale_factor)
        else:
            estimated_duplicates = total_duplicates
            estimated_groups = len(groups)

        # Determine severity based on duplicate percentage
        duplicate_pct = estimated_duplicates / original_size * 100
        if duplicate_pct > 10:
            severity = Severity.HIGH
        elif duplicate_pct > 5:
            severity = Severity.MEDIUM
        else:
            severity = Severity.LOW

        # Create a single finding summarizing fuzzy duplicates
        finding = Finding(
            detector=self.name,
            issue_type="fuzzy_duplicates",
            severity=severity,
            columns=columns,
            details={
                "duplicate_groups": estimated_groups,
                "duplicate_rows": estimated_duplicates,
                "duplicate_percentage": round(duplicate_pct, 2),
                "threshold": self._threshold,
                "sample_groups": groups[:10],  # Include first 10 groups as examples
                "columns_compared": columns,
            },
            message=(
                f"Found ~{estimated_groups} groups of semantically similar rows "
                f"(~{estimated_duplicates} potential duplicates, {duplicate_pct:.1f}% of data)"
            ),
            confidence=min(0.95, 0.7 + duplicate_pct / 100),
        )

        return [finding]


# Only register if ML is available
try:
    from datawash.ml import ML_AVAILABLE

    if ML_AVAILABLE:
        register_detector(FuzzyDuplicateDetector())
except ImportError:
    pass
