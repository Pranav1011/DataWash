"""Column operations (merge, rename, drop)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class ColumnTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "columns"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        operation = params.get("operation", "drop")
        columns = params.get("columns", [])
        result_df = df.copy()
        affected = 0

        if operation == "drop":
            existing = [c for c in columns if c in result_df.columns]
            result_df = result_df.drop(columns=existing)
            affected = len(result_df) * len(existing)
        elif operation == "rename":
            mapping = params.get("mapping", {})
            result_df = result_df.rename(columns=mapping)
            affected = len(result_df) * len(mapping)
        elif operation == "merge":
            if len(columns) >= 2:
                new_name = params.get("new_name", "_".join(columns))
                separator = params.get("separator", " ")
                result_df[new_name] = (
                    result_df[columns].astype(str).agg(separator.join, axis=1)
                )
                result_df = result_df.drop(columns=columns)
                affected = len(result_df)

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        operation = params.get("operation", "drop")
        columns = params.get("columns", [])
        if operation == "drop":
            return f"df = df.drop(columns={repr(columns)})"
        elif operation == "rename":
            mapping = params.get("mapping", {})
            return f"df = df.rename(columns={repr(mapping)})"
        elif operation == "merge":
            new_name = params.get("new_name", "_".join(columns))
            sep = params.get("separator", " ")
            return (
                f"df[{repr(new_name)}] = df[{repr(columns)}]"
                f".astype(str).agg({repr(sep)}.join, axis=1)\n"
                f"df = df.drop(columns={repr(columns)})"
            )
        return ""


register_transformer(ColumnTransformer())
