"""Tests for code generation."""

from __future__ import annotations
import pytest
from datawash.core.models import TransformationResult
from datawash.codegen import generate_code


@pytest.fixture
def sample_results() -> list[TransformationResult]:
    return [
        TransformationResult(
            transformer="duplicates",
            params={"keep": "first"},
            rows_affected=5,
            columns_affected=["a", "b"],
            code='df = df.drop_duplicates(keep="first")',
        ),
        TransformationResult(
            transformer="missing",
            params={"strategy": "drop_rows", "columns": ["a"]},
            rows_affected=3,
            columns_affected=["a"],
            code="df = df.dropna(subset=['a'])",
        ),
    ]


def test_generate_function_style(sample_results: list[TransformationResult]) -> None:
    code = generate_code(sample_results, style="function")
    assert "def clean_data" in code
    assert "drop_duplicates" in code
    assert "dropna" in code
    assert "import pandas" in code


def test_generate_script_style(sample_results: list[TransformationResult]) -> None:
    code = generate_code(sample_results, style="script")
    assert "def clean_data" not in code
    assert "read_csv" in code
    assert "to_csv" in code


def test_empty_results() -> None:
    code = generate_code([])
    assert "No transformations" in code


def test_no_comments(sample_results: list[TransformationResult]) -> None:
    code = generate_code(sample_results, include_comments=False)
    assert "# duplicates:" not in code
