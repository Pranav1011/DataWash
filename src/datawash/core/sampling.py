"""Smart sampling for large datasets."""

from __future__ import annotations

import pandas as pd

SAMPLE_THRESHOLD = 50_000
SAMPLE_SIZE = 10_000


class SmartSampler:
    """Intelligent sampling for large datasets.

    For datasets above ``SAMPLE_THRESHOLD`` rows, creates a representative
    sample that preserves data distribution and edge cases (nulls, outliers).
    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.original_df = df
        self.original_size = len(df)
        self.is_sampled = len(df) > SAMPLE_THRESHOLD
        self.sample_df = self._create_sample() if self.is_sampled else df
        self.scale_factor = (
            self.original_size / len(self.sample_df) if self.is_sampled else 1.0
        )

    def _create_sample(self) -> pd.DataFrame:
        """Create representative sample preserving data distribution."""
        df = self.original_df

        # Always include rows with nulls (up to 10% of sample)
        null_rows = df[df.isna().any(axis=1)]
        max_null_rows = SAMPLE_SIZE // 10
        if len(null_rows) > max_null_rows:
            null_sample = null_rows.sample(max_null_rows, random_state=42)
        elif len(null_rows) > 0:
            null_sample = null_rows
        else:
            null_sample = df.iloc[:0]

        # Sample remaining rows
        remaining_size = SAMPLE_SIZE - len(null_sample)
        non_null_rows = df.drop(null_sample.index, errors="ignore")

        # Try stratified sampling on first low-cardinality column
        strat_col = self._find_stratification_column(non_null_rows)
        if strat_col:
            main_sample = self._stratified_sample(
                non_null_rows, strat_col, remaining_size
            )
        else:
            actual_size = min(remaining_size, len(non_null_rows))
            main_sample = non_null_rows.sample(actual_size, random_state=42)

        return pd.concat([null_sample, main_sample]).reset_index(drop=True)

    def _find_stratification_column(self, df: pd.DataFrame) -> str | None:
        """Find a good column for stratified sampling."""
        for col in df.columns:
            if df[col].isna().all():
                continue
            nunique = df[col].nunique()
            if 2 <= nunique <= 20:
                return col
        return None

    def _stratified_sample(
        self, df: pd.DataFrame, strat_col: str, n: int
    ) -> pd.DataFrame:
        """Stratified sampling proportional to group sizes."""
        groups = df.groupby(strat_col, group_keys=False, observed=True)
        total = len(df)
        parts = []
        for _name, group in groups:
            group_n = max(1, int(n * len(group) / total))
            group_n = min(group_n, len(group))
            parts.append(group.sample(group_n, random_state=42))
        return pd.concat(parts)

    def extrapolate_count(self, sample_count: int) -> int:
        """Scale sample count to full dataset estimate."""
        if not self.is_sampled:
            return sample_count
        return int(sample_count * self.scale_factor)
