"""Transformer registration."""

from __future__ import annotations

from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer

_TRANSFORMERS: dict[str, BaseTransformer] = {}


def register_transformer(t: BaseTransformer) -> None:
    _TRANSFORMERS[t.name] = t


def get_transformer(name: str) -> BaseTransformer:
    if name not in _TRANSFORMERS:
        raise KeyError(
            f"Unknown transformer: {name}. Available: {list(_TRANSFORMERS.keys())}"
        )
    return _TRANSFORMERS[name]


def run_transformer(
    name: str, df: pd.DataFrame, **params: Any
) -> tuple[pd.DataFrame, TransformationResult]:
    return get_transformer(name).transform(df, **params)
