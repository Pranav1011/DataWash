"""Remove fuzzy (semantic) duplicate rows.

Uses ML embeddings to identify and remove semantically similar rows,
keeping the most complete or first occurrence.

Requires ML dependencies: pip install datawash[ml]
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer

logger = logging.getLogger(__name__)


class FuzzyDuplicateTransformer(BaseTransformer):
    """Remove semantically similar (fuzzy) duplicate rows.

    Uses sentence embeddings to identify rows that are semantically similar
    and removes duplicates based on a configurable strategy.

    Requires: pip install datawash[ml]
    """

    @property
    def name(self) -> str:
        return "fuzzy_duplicates"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        """Remove fuzzy duplicate rows.

        Args:
            df: Input DataFrame.
            **params: Parameters:
                - columns: Columns to compare for similarity.
                - threshold: Similarity threshold (0-1). Default 0.85.
                - strategy: How to pick canonical row: "first", "most_complete", "longest".
                - keep: Alias for strategy for compatibility.

        Returns:
            Tuple of (cleaned DataFrame, TransformationResult).
        """
        columns = params.get("columns", None)
        threshold = params.get("threshold", 0.85)
        strategy = params.get("strategy", params.get("keep", "first"))

        # Check ML availability
        try:
            from datawash.ml import ML_AVAILABLE

            if not ML_AVAILABLE:
                logger.warning(
                    "ML dependencies not available, cannot remove fuzzy duplicates"
                )
                return df.copy(), TransformationResult(
                    transformer=self.name,
                    params=params,
                    rows_affected=0,
                    columns_affected=[],
                    code=self.generate_code(**params),
                )
        except ImportError:
            logger.warning("ML module not available, cannot remove fuzzy duplicates")
            return df.copy(), TransformationResult(
                transformer=self.name,
                params=params,
                rows_affected=0,
                columns_affected=[],
                code=self.generate_code(**params),
            )

        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        before = len(df)
        result_df = df.copy()

        try:
            finder = FuzzyDuplicateFinder(model_preset="fast")
            groups = finder.find_duplicates(
                result_df,
                columns=columns,
                threshold=threshold,
            )

            if not groups:
                return result_df, TransformationResult(
                    transformer=self.name,
                    params=params,
                    rows_affected=0,
                    columns_affected=columns or list(df.columns),
                    code=self.generate_code(**params),
                )

            # Collect indices to drop
            indices_to_drop = set()
            for group in groups:
                # Find the canonical row to keep
                canonical_idx = finder.suggest_canonical(
                    result_df,
                    group,
                    columns=columns,
                    strategy=strategy,
                )
                # Mark all others for removal
                for idx in group:
                    if idx != canonical_idx:
                        indices_to_drop.add(result_df.index[idx])

            # Drop the duplicates
            result_df = result_df.drop(index=list(indices_to_drop))
            after = len(result_df)

            return result_df, TransformationResult(
                transformer=self.name,
                params=params,
                rows_affected=before - after,
                columns_affected=columns or list(df.columns),
                code=self.generate_code(**params),
            )

        except Exception as e:
            logger.warning("Fuzzy duplicate removal failed: %s", e)
            return df.copy(), TransformationResult(
                transformer=self.name,
                params=params,
                rows_affected=0,
                columns_affected=[],
                code=self.generate_code(**params),
            )

    def generate_code(self, **params: Any) -> str:
        """Generate equivalent Python code.

        Note: This generates a comment explaining manual steps since
        the operation requires ML dependencies.
        """
        columns = params.get("columns", None)
        threshold = params.get("threshold", 0.85)
        strategy = params.get("strategy", params.get("keep", "first"))

        lines = [
            "# Remove fuzzy (semantic) duplicates",
            "# Requires: pip install datawash[ml]",
            "from datawash.ml import FuzzyDuplicateFinder",
            "",
            "finder = FuzzyDuplicateFinder()",
        ]

        if columns:
            lines.append(
                f"groups = finder.find_duplicates(df, columns={columns}, threshold={threshold})"
            )
        else:
            lines.append(f"groups = finder.find_duplicates(df, threshold={threshold})")

        lines.extend(
            [
                "",
                "# Remove duplicates, keeping canonical row per group",
                "indices_to_drop = set()",
                "for group in groups:",
                f"    canonical = finder.suggest_canonical(df, group, strategy='{strategy}')",
                "    for idx in group:",
                "        if idx != canonical:",
                "            indices_to_drop.add(df.index[idx])",
                "",
                "df = df.drop(index=list(indices_to_drop))",
            ]
        )

        return "\n".join(lines)


# Only register if ML is available
try:
    from datawash.ml import ML_AVAILABLE

    if ML_AVAILABLE:
        register_transformer(FuzzyDuplicateTransformer())
except ImportError:
    pass
