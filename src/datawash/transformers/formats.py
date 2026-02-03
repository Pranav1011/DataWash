"""Format standardization transformers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class FormatTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "formats"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        columns = params.get("columns", [])
        operation = params.get("operation", "strip_whitespace")
        result_df = df.copy()
        affected = 0

        for col in columns:
            if col not in result_df.columns:
                continue
            if operation == "strip_whitespace":
                before = result_df[col].copy()
                result_df[col] = result_df[col].astype(str).str.strip()
                affected += int((before != result_df[col]).sum())
            elif operation == "lowercase":
                before = result_df[col].copy()
                result_df[col] = result_df[col].astype(str).str.lower()
                affected += int((before != result_df[col]).sum())
            elif operation == "uppercase":
                before = result_df[col].copy()
                result_df[col] = result_df[col].astype(str).str.upper()
                affected += int((before != result_df[col]).sum())
            elif operation == "titlecase":
                before = result_df[col].copy()
                result_df[col] = result_df[col].astype(str).str.title()
                affected += int((before != result_df[col]).sum())
            elif operation == "standardize_dates":
                target_format = params.get("target_format", "%Y-%m-%d")
                parsed = pd.to_datetime(result_df[col], errors="coerce")
                affected += int(parsed.notna().sum())
                result_df[col] = parsed.dt.strftime(target_format).where(
                    parsed.notna(), result_df[col]
                )

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        columns = params.get("columns", [])
        operation = params.get("operation", "strip_whitespace")
        lines = []
        for col in columns:
            if operation == "strip_whitespace":
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].astype(str).str.strip()"
                )
            elif operation == "lowercase":
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].astype(str).str.lower()"
                )
            elif operation == "uppercase":
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].astype(str).str.upper()"
                )
            elif operation == "titlecase":
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].astype(str).str.title()"
                )
            elif operation == "standardize_dates":
                fmt = params.get("target_format", "%Y-%m-%d")
                lines.append(
                    f"_parsed = pd.to_datetime(df[{repr(col)}], errors='coerce')"
                )
                lines.append(
                    f"df[{repr(col)}] = _parsed.dt.strftime({repr(fmt)})"
                    f".where(_parsed.notna(), df[{repr(col)}])"
                )
        return "\n".join(lines)


register_transformer(FormatTransformer())
