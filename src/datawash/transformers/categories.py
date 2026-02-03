"""Category normalization."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class CategoryTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "categories"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        columns = params.get("columns", [])
        mapping = params.get("mapping", {})
        result_df = df.copy()
        affected = 0

        for col in columns:
            if col not in result_df.columns:
                continue
            if mapping:
                mask = result_df[col].isin(mapping.keys())
                affected += int(mask.sum())
                result_df[col] = result_df[col].replace(mapping)
            else:
                # Auto-normalize: strip + lowercase
                before = result_df[col].copy()
                result_df[col] = result_df[col].astype(str).str.strip().str.lower()
                affected += int((before != result_df[col]).sum())

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        columns = params.get("columns", [])
        mapping = params.get("mapping", {})
        lines = []
        for col in columns:
            if mapping:
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].replace({repr(mapping)})"
                )
            else:
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].astype(str).str.strip().str.lower()"
                )
        return "\n".join(lines)


register_transformer(CategoryTransformer())
