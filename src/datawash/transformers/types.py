"""Type conversion transformers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class TypeTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "types"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        columns = params.get("columns", [])
        target_type = params.get("target_type", "numeric")
        result_df = df.copy()
        affected = 0

        for col in columns:
            if col not in result_df.columns:
                continue
            if target_type == "numeric":
                converted = pd.to_numeric(result_df[col], errors="coerce")
                affected += int((converted != result_df[col].astype(str)).sum())
                result_df[col] = converted
            elif target_type == "boolean":
                bool_map = {
                    "true": True,
                    "false": False,
                    "yes": True,
                    "no": False,
                    "y": True,
                    "n": False,
                    "1": True,
                    "0": False,
                    "t": True,
                    "f": False,
                    "on": True,
                    "off": False,
                }
                result_df[col] = result_df[col].astype(str).str.lower().map(bool_map)
                affected += len(result_df[col].dropna())
            elif target_type == "datetime":
                result_df[col] = pd.to_datetime(result_df[col], errors="coerce")
                affected += int(result_df[col].notna().sum())
            elif target_type == "string":
                result_df[col] = result_df[col].astype(str)
                affected += len(result_df[col])

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        columns = params.get("columns", [])
        target_type = params.get("target_type", "numeric")
        lines = []
        for col in columns:
            if target_type == "numeric":
                lines.append(
                    f"df[{repr(col)}] = pd.to_numeric(df[{repr(col)}], errors='coerce')"
                )
            elif target_type == "boolean":
                bmap = (
                    "{'true': True, 'false': False, "
                    "'yes': True, 'no': False, "
                    "'y': True, 'n': False, "
                    "'1': True, '0': False}"
                )
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}]"
                    f".astype(str).str.lower().map({bmap})"
                )
            elif target_type == "datetime":
                lines.append(
                    f"df[{repr(col)}] = pd.to_datetime(df[{repr(col)}], errors='coerce')"
                )
            elif target_type == "string":
                lines.append(f"df[{repr(col)}] = df[{repr(col)}].astype(str)")
        return "\n".join(lines)


register_transformer(TypeTransformer())
