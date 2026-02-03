"""Tests for suggestion engine."""
from __future__ import annotations
import pandas as pd
import pytest
from datawash.profiler import profile_dataset
from datawash.detectors import run_all_detectors
from datawash.suggestors import generate_suggestions


def test_generates_suggestions(messy_df: pd.DataFrame) -> None:
    profile = profile_dataset(messy_df)
    findings = run_all_detectors(messy_df, profile)
    suggestions = generate_suggestions(findings)
    assert len(suggestions) > 0
    # IDs should be sequential
    ids = [s.id for s in suggestions]
    assert ids == list(range(1, len(suggestions) + 1))


def test_suggestions_are_prioritized(messy_df: pd.DataFrame) -> None:
    profile = profile_dataset(messy_df)
    findings = run_all_detectors(messy_df, profile)
    suggestions = generate_suggestions(findings)
    # High priority should come first
    priorities = [s.priority.value for s in suggestions]
    assert priorities[0] in ("high", "medium")


def test_max_suggestions_limit(messy_df: pd.DataFrame) -> None:
    profile = profile_dataset(messy_df)
    findings = run_all_detectors(messy_df, profile)
    suggestions = generate_suggestions(findings, max_suggestions=2)
    assert len(suggestions) <= 2


def test_no_findings_no_suggestions() -> None:
    suggestions = generate_suggestions([])
    assert len(suggestions) == 0
