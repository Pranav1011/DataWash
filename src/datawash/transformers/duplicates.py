"""Remove duplicate rows."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class DuplicateTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "duplicates"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        keep = params.get("keep", "first")
        subset = params.get("subset", None)
        before = len(df)
        result_df = df.drop_duplicates(keep=keep, subset=subset)
        after = len(result_df)
        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=before - after,
            columns_affected=list(df.columns),
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        keep = params.get("keep", "first")
        subset = params.get("subset", None)
        if subset:
            return f'df = df.drop_duplicates(keep="{keep}", subset={subset})'
        return f'df = df.drop_duplicates(keep="{keep}")'


register_transformer(DuplicateTransformer())
