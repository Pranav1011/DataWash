"""Integration tests for end-to-end workflows."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from datawash import analyze
from datawash.core import report as report_module
from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.core.report import Report
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import _DETECTORS


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
    assert report.last_score_before is not None
    assert report.last_score_after is not None
    assert report.last_score_after >= report.last_score_before


class _AlwaysFindsDetector(BaseDetector):
    """Stand-in for the ML-only fuzzy duplicate detector."""

    @property
    def name(self) -> str:
        return "fuzzy_duplicates"

    @property
    def description(self) -> str:
        return "Test stub that always reports one issue"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        return [
            Finding(
                detector=self.name,
                issue_type="fuzzy_duplicates",
                severity=Severity.HIGH,
                message="stub finding",
            )
        ]


def test_scores_are_none_before_apply(messy_df: pd.DataFrame) -> None:
    report = analyze(messy_df)
    assert report.last_score_before is None
    assert report.last_score_after is None
    report.apply_all()
    assert report.last_score_before == report._last_score_before
    assert report.last_score_after == report._last_score_after


@pytest.mark.parametrize("parallel", [True, False])
def test_apply_nothing_keeps_score_with_ml_features(
    messy_df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch, parallel: bool
) -> None:
    """The after-score uses the same detectors as the before-score."""
    monkeypatch.setitem(_DETECTORS, "fuzzy_duplicates", _AlwaysFindsDetector())
    report = analyze(messy_df, ml_features=True, parallel=parallel)
    assert report._compute_quality_score(report.df) == report.quality_score
    report.apply([])
    assert report.last_score_after == report.last_score_before


def test_apply_nothing_keeps_score_on_sampled_data() -> None:
    """The after-score samples large data the same way the before-score does."""
    rng = np.random.default_rng(0)
    n = 57_000
    df = pd.DataFrame(
        {
            "id": np.arange(n),
            "value": rng.normal(size=n),
            "label": rng.choice(["a", "b", "c"], n),
        }
    )
    # 5% duplicates in full data, far fewer inside the 10k-row sample
    df = pd.concat([df, df.iloc[:3000]], ignore_index=True)
    report = analyze(df)
    assert report.profile.sampled
    assert report._compute_quality_score(report.df) == report.quality_score
    report.apply([])
    assert report.last_score_after == report.last_score_before


def test_failed_apply_records_partial_state(
    messy_df: pd.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An error mid-apply must not make generate_code() apply everything."""
    report = analyze(messy_df)
    ids = [s.id for s in report.suggestions[:2]]
    real_run = report_module.run_transformer
    calls = iter([real_run, None])

    def flaky_run(*args: object, **kwargs: object) -> object:
        run = next(calls)
        if run is None:
            raise RuntimeError("transformer failed")
        return run(*args, **kwargs)

    monkeypatch.setattr(report_module, "run_transformer", flaky_run)
    with pytest.raises(RuntimeError):
        report.apply(ids)
    assert report._applied is not None
    assert len(report._applied) == 1
    assert report.last_score_after is None


def test_apply_interactive_skip_all_keeps_score(messy_df: pd.DataFrame) -> None:
    report = analyze(messy_df)
    report.apply_interactive(input_fn=lambda _prompt: "q")
    assert report.last_score_after == report.last_score_before
