"""Integration tests for end-to-end workflows."""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import pytest
from datawash import analyze
from datawash.core.report import Report


def test_full_workflow(messy_df: pd.DataFrame) -> None:
    """Test: analyze -> suggest -> apply -> codegen."""
    report = analyze(messy_df)

    assert report.profile.row_count == 10
    assert len(report.issues) > 0
    assert len(report.suggestions) > 0

    clean_df = report.apply_all()
    assert len(clean_df) <= len(messy_df)

    code = report.generate_code()
    assert "import pandas" in code
    assert len(code) > 50


def test_selective_apply(messy_df: pd.DataFrame) -> None:
    """Test applying specific suggestions."""
    report = analyze(messy_df)
    if report.suggestions:
        clean_df = report.apply([report.suggestions[0].id])
        assert isinstance(clean_df, pd.DataFrame)


def test_csv_roundtrip(tmp_path: Path, messy_df: pd.DataFrame) -> None:
    """Test: save CSV -> analyze -> clean -> save."""
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    messy_df.to_csv(input_path, index=False)

    report = analyze(str(input_path))
    clean_df = report.apply_all()
    clean_df.to_csv(output_path, index=False)

    result = pd.read_csv(output_path)
    assert len(result) <= len(messy_df)


def test_summary(messy_df: pd.DataFrame) -> None:
    report = analyze(messy_df)
    summary = report.summary()
    assert "rows" in summary.lower()
    assert "issues" in summary.lower()


def test_repr(messy_df: pd.DataFrame) -> None:
    report = analyze(messy_df)
    r = repr(report)
    assert "Report(" in r


def test_repr_html(messy_df: pd.DataFrame) -> None:
    report = analyze(messy_df)
    html = report._repr_html_()
    assert "<div" in html
    assert "DataWash" in html


def test_apply_interactive_apply_all(messy_df: pd.DataFrame) -> None:
    """Test apply_interactive with 'A' (apply all) on first prompt."""
    report = analyze(messy_df)
    inputs = iter(["A"])
    clean_df = report.apply_interactive(input_fn=lambda _prompt: next(inputs))
    assert isinstance(clean_df, pd.DataFrame)
    assert len(report._applied) > 0


def test_apply_interactive_skip_then_quit(messy_df: pd.DataFrame) -> None:
    """Test apply_interactive with skip then quit."""
    report = analyze(messy_df)
    responses = iter(["s", "q"])
    clean_df = report.apply_interactive(input_fn=lambda _prompt: next(responses))
    assert isinstance(clean_df, pd.DataFrame)
    # Should have applied 0 suggestions (skipped first, quit on second)
    assert len(report._applied) == 0


def test_apply_interactive_apply_one(messy_df: pd.DataFrame) -> None:
    """Test apply_interactive applying one then quitting."""
    report = analyze(messy_df)
    responses = iter(["a", "q"])
    clean_df = report.apply_interactive(input_fn=lambda _prompt: next(responses))
    assert isinstance(clean_df, pd.DataFrame)
    assert len(report._applied) == 1


def test_apply_stores_quality_scores(messy_df: pd.DataFrame) -> None:
    """Test that apply() stores before/after quality scores."""
    report = analyze(messy_df)
    report.apply_all()
    assert hasattr(report, "_last_score_before")
    assert hasattr(report, "_last_score_after")
    assert report._last_score_after >= report._last_score_before
