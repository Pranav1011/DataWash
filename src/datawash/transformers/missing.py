"""Handle missing values."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer

logger = logging.getLogger(__name__)


class MissingTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "missing"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        strategy = params.get("strategy", "drop_rows")
        columns = params.get("columns", list(df.columns))
        result_df = df.copy()
        rows_before = len(result_df)
        affected = 0

        if strategy == "drop_rows":
            result_df = result_df.dropna(subset=columns)
            affected = rows_before - len(result_df)
        elif strategy == "fill_median":
            for col in columns:
                if pd.api.types.is_numeric_dtype(result_df[col]):
                    median = result_df[col].median()
                    affected += int(result_df[col].isna().sum())
                    result_df[col] = result_df[col].fillna(median)
        elif strategy == "fill_mode":
            for col in columns:
                mode = result_df[col].mode()
                if not mode.empty:
                    affected += int(result_df[col].isna().sum())
                    result_df[col] = result_df[col].fillna(mode.iloc[0])
                else:
                    logger.warning(
                        "Column '%s': fill_mode requested but no mode found "
                        "(all values null). Column left unchanged.",
                        col,
                    )
        elif strategy == "fill_value":
            fill_value = params.get("fill_value", "")
            for col in columns:
                affected += int(result_df[col].isna().sum())
                result_df[col] = result_df[col].fillna(fill_value)
        elif strategy == "empty_to_nan":
            for col in columns:
                mask = result_df[col] == ""
                affected += int(mask.sum())
                result_df.loc[mask, col] = np.nan
        elif strategy == "clean_empty_strings":
            # Combined strategy: convert empty/whitespace strings to NaN and fill in one step
            fill_strategy = params.get("fill_strategy", "mode")
            for col in columns:
                # Convert empty and whitespace-only strings to NaN
                # Handle both 'object' and string dtypes
                col_dtype = result_df[col].dtype
                is_string_like = col_dtype == object or pd.api.types.is_string_dtype(
                    col_dtype
                )
                if is_string_like:
                    mask = result_df[col].apply(
                        lambda x: isinstance(x, str) and x.strip() == ""
                    )
                    empty_count = int(mask.sum())
                    result_df.loc[mask, col] = np.nan
                else:
                    empty_count = 0

                # Now fill NaN values
                null_count = int(result_df[col].isna().sum())
                if null_count > 0:
                    if fill_strategy == "mode":
                        mode = result_df[col].mode()
                        if not mode.empty:
                            result_df[col] = result_df[col].fillna(mode.iloc[0])
                    elif fill_strategy == "median":
                        if pd.api.types.is_numeric_dtype(result_df[col]):
                            result_df[col] = result_df[col].fillna(
                                result_df[col].median()
                            )
                    elif fill_strategy == "value":
                        fill_value = params.get("fill_value", "")
                        result_df[col] = result_df[col].fillna(fill_value)

                affected += max(empty_count, null_count)
        elif strategy == "clip_outliers":
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)
            for col in columns:
                if not pd.api.types.is_numeric_dtype(result_df[col]):
                    continue
                series = result_df[col].dropna()
                if method == "iqr":
                    q1, q3 = series.quantile(0.25), series.quantile(0.75)
                    iqr = q3 - q1
                    lower, upper = q1 - threshold * iqr, q3 + threshold * iqr
                else:
                    mean, std = series.mean(), series.std()
                    lower, upper = mean - threshold * std, mean + threshold * std
                mask = (result_df[col] < lower) | (result_df[col] > upper)
                affected += int(mask.sum())
                result_df[col] = result_df[col].clip(lower=lower, upper=upper)

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        strategy = params.get("strategy", "drop_rows")
        columns = params.get("columns", [])
        col_repr = repr(columns)
        if strategy == "drop_rows":
            return f"df = df.dropna(subset={col_repr})"
        elif strategy == "fill_median":
            lines = [
                f"df[{repr(c)}] = df[{repr(c)}].fillna(df[{repr(c)}].median())"
                for c in columns
            ]
            return "\n".join(lines)
        elif strategy == "fill_mode":
            lines = [
                f"df[{repr(c)}] = df[{repr(c)}].fillna(df[{repr(c)}].mode().iloc[0])"
                for c in columns
            ]
            return "\n".join(lines)
        elif strategy == "fill_value":
            val = repr(params.get("fill_value", ""))
            lines = [f"df[{repr(c)}] = df[{repr(c)}].fillna({val})" for c in columns]
            return "\n".join(lines)
        elif strategy == "empty_to_nan":
            lines = [
                f"df[{repr(c)}] = df[{repr(c)}].replace('', np.nan)" for c in columns
            ]
            return "import numpy as np\n" + "\n".join(lines)
        elif strategy == "clean_empty_strings":
            fill_strategy = params.get("fill_strategy", "mode")
            lines = ["import numpy as np"]
            for c in columns:
                # Convert empty/whitespace to NaN
                lines.append(
                    f"df[{repr(c)}] = df[{repr(c)}].replace(r'^\\s*$', np.nan, regex=True)"
                )
                # Fill based on strategy
                if fill_strategy == "mode":
                    lines.append(
                        f"df[{repr(c)}] = df[{repr(c)}].fillna(df[{repr(c)}].mode().iloc[0])"
                    )
                elif fill_strategy == "median":
                    lines.append(
                        f"df[{repr(c)}] = df[{repr(c)}].fillna(df[{repr(c)}].median())"
                    )
                elif fill_strategy == "value":
                    val = repr(params.get("fill_value", ""))
                    lines.append(f"df[{repr(c)}] = df[{repr(c)}].fillna({val})")
            return "\n".join(lines)
        elif strategy == "clip_outliers":
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)
            lines = []
            for c in columns:
                if method == "iqr":
                    lines.append(
                        f"q1, q3 = df[{repr(c)}].quantile(0.25), df[{repr(c)}].quantile(0.75)"
                    )
                    lines.append("iqr = q3 - q1")
                    lines.append(
                        f"df[{repr(c)}] = df[{repr(c)}].clip("
                        f"lower=q1 - {threshold} * iqr, "
                        f"upper=q3 + {threshold} * iqr)"
                    )
                else:
                    lines.append(
                        f"mean, std = df[{repr(c)}].mean(), " f"df[{repr(c)}].std()"
                    )
                    lines.append(
                        f"df[{repr(c)}] = df[{repr(c)}].clip("
                        f"lower=mean - {threshold} * std, "
                        f"upper=mean + {threshold} * std)"
                    )
            return "\n".join(lines)
        return "# No code generated"


register_transformer(MissingTransformer())
