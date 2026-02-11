"""Fuzzy duplicate detection using semantic embeddings."""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from datawash.ml.embeddings import TextEmbedder
from datawash.ml.similarity import SimilaritySearch

if TYPE_CHECKING:
    from numpy.typing import NDArray


class FuzzyDuplicateFinder:
    """Find semantically similar (fuzzy) duplicates in a DataFrame.

    Uses sentence embeddings to identify rows that are semantically similar
    but not exact duplicates. This catches variations like:
    - "John Smith" vs "Jon Smith" (typos)
    - "123 Main St" vs "123 Main Street" (abbreviations)
    - "NYC" vs "New York City" (synonyms)

    Example:
        >>> finder = FuzzyDuplicateFinder()
        >>> df = pd.DataFrame({
        ...     "name": ["John Smith", "Jon Smith", "Jane Doe"],
        ...     "city": ["New York", "NYC", "Boston"]
        ... })
        >>> groups = finder.find_duplicates(df, columns=["name"], threshold=0.85)
        >>> print(groups)  # [[0, 1]]  - indices of similar rows
    """

    def __init__(
        self,
        model_preset: str = "fast",
        model_name: str | None = None,
        device: str | None = None,
    ):
        """Initialize the fuzzy duplicate finder.

        Args:
            model_preset: One of "fast", "balanced", "multilingual". Default "fast".
            model_name: Specific model name, overrides preset.
            device: Device for embeddings ("cpu", "cuda", "mps"). Auto-detected if None.
        """
        self._embedder = TextEmbedder(
            model_name=model_name,
            model_preset=model_preset,
            device=device,
        )
        self._last_embeddings: NDArray[np.float32] | None = None
        self._last_texts: list[str] | None = None

    @property
    def embedder(self) -> TextEmbedder:
        """The underlying text embedder."""
        return self._embedder

    def _prepare_text(
        self,
        df: pd.DataFrame,
        columns: list[str] | None = None,
        sep: str = " | ",
    ) -> list[str]:
        """Prepare text representations of rows.

        Args:
            df: Input DataFrame.
            columns: Columns to use. If None, uses all string columns.
            sep: Separator between column values.

        Returns:
            List of text representations, one per row.
        """
        if columns is None:
            # Use all string/object columns
            columns = df.select_dtypes(include=["object", "string"]).columns.tolist()

        if not columns:
            raise ValueError("No text columns found or specified")

        # Validate columns exist
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found: {missing}")

        # Combine column values into single text per row
        texts = []
        for _, row in df[columns].iterrows():
            parts = [str(v) if pd.notna(v) else "" for v in row]
            texts.append(sep.join(parts))

        return texts

    def find_duplicates(
        self,
        df: pd.DataFrame,
        columns: list[str] | None = None,
        threshold: float = 0.85,
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> list[list[int]]:
        """Find groups of semantically similar rows.

        Args:
            df: Input DataFrame.
            columns: Columns to compare. If None, uses all string columns.
            threshold: Similarity threshold (0-1). Default 0.85.
            batch_size: Batch size for embedding generation.
            show_progress: Show progress bar for embeddings.

        Returns:
            List of duplicate groups, where each group is a list of row indices.
        """
        if len(df) < 2:
            return []

        # Prepare texts
        texts = self._prepare_text(df, columns)
        self._last_texts = texts

        # Generate embeddings
        embeddings = self._embedder.embed(
            texts,
            batch_size=batch_size,
            show_progress=show_progress,
            normalize=True,
        )
        self._last_embeddings = embeddings

        # Build search index and find duplicates
        search = SimilaritySearch(embeddings, normalize=False)  # Already normalized
        pairs = search.find_duplicates(threshold=threshold)

        # Convert pairs to groups using Union-Find
        return self._pairs_to_groups(pairs, len(df))

    def _pairs_to_groups(
        self,
        pairs: list[tuple[int, int, float]],
        n: int,
    ) -> list[list[int]]:
        """Convert duplicate pairs to groups using Union-Find.

        Args:
            pairs: List of (idx1, idx2, similarity) tuples.
            n: Total number of items.

        Returns:
            List of groups, each containing indices of similar items.
        """
        # Union-Find data structure
        parent = list(range(n))
        rank = [0] * n

        def find(x: int) -> int:
            if parent[x] != x:
                parent[x] = find(parent[x])  # Path compression
            return parent[x]

        def union(x: int, y: int) -> None:
            px, py = find(x), find(y)
            if px == py:
                return
            # Union by rank
            if rank[px] < rank[py]:
                px, py = py, px
            parent[py] = px
            if rank[px] == rank[py]:
                rank[px] += 1

        # Union all pairs
        for idx1, idx2, _ in pairs:
            union(idx1, idx2)

        # Collect groups
        groups: dict[int, list[int]] = {}
        for i in range(n):
            root = find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)

        # Return only groups with more than one member
        return [sorted(g) for g in groups.values() if len(g) > 1]

    def find_duplicates_for_columns(
        self,
        df: pd.DataFrame,
        column_groups: list[list[str]],
        threshold: float = 0.85,
    ) -> dict[str, list[list[int]]]:
        """Find duplicates for multiple column groupings.

        Args:
            df: Input DataFrame.
            column_groups: List of column groups to check separately.
            threshold: Similarity threshold.

        Returns:
            Dictionary mapping column group key to duplicate groups.
        """
        results = {}
        for columns in column_groups:
            key = ",".join(sorted(columns))
            groups = self.find_duplicates(df, columns=columns, threshold=threshold)
            if groups:
                results[key] = groups
        return results

    def get_duplicate_summary(
        self,
        df: pd.DataFrame,
        groups: list[list[int]],
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Get a summary DataFrame showing duplicate groups.

        Args:
            df: Original DataFrame.
            groups: Duplicate groups from find_duplicates().
            columns: Columns to include in summary.

        Returns:
            DataFrame with group_id, row_index, and specified columns.
        """
        if columns is None:
            columns = df.columns.tolist()

        rows = []
        for group_id, group in enumerate(groups):
            for idx in group:
                row_data = {"_group_id": group_id, "_row_index": idx}
                for col in columns:
                    if col in df.columns:
                        row_data[col] = df.iloc[idx][col]
                rows.append(row_data)

        return pd.DataFrame(rows)

    def suggest_canonical(
        self,
        df: pd.DataFrame,
        group: list[int],
        columns: list[str] | None = None,
        strategy: str = "first",
    ) -> int:
        """Suggest which row should be the canonical (kept) row.

        Args:
            df: Original DataFrame.
            group: List of row indices in the duplicate group.
            columns: Columns to consider for completeness.
            strategy: One of "first", "most_complete", "longest".

        Returns:
            Index of the suggested canonical row.
        """
        if strategy == "first":
            return group[0]

        if columns is None:
            columns = df.columns.tolist()

        if strategy == "most_complete":
            # Row with fewest nulls
            null_counts = []
            for idx in group:
                null_count = df.iloc[idx][columns].isna().sum()
                null_counts.append((null_count, idx))
            return min(null_counts)[1]

        if strategy == "longest":
            # Row with longest total text
            lengths = []
            for idx in group:
                total_len = sum(
                    len(str(v)) if pd.notna(v) else 0 for v in df.iloc[idx][columns]
                )
                lengths.append((-total_len, idx))  # Negative for max
            return min(lengths)[1]

        return group[0]

    def get_row_hash(self, df: pd.DataFrame, idx: int, columns: list[str]) -> str:
        """Get a hash for a row's content.

        Args:
            df: DataFrame.
            idx: Row index.
            columns: Columns to include in hash.

        Returns:
            SHA256 hash of the row content.
        """
        values = [
            str(df.iloc[idx][c]) if pd.notna(df.iloc[idx][c]) else "" for c in columns
        ]
        content = "|".join(values)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
