"""Tests for suggestion engine."""

from __future__ import annotations

import pandas as pd
import pytest

from datawash.detectors import run_all_detectors
from datawash.profiler import profile_dataset
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


def test_missing_strategy_numeric_column() -> None:
    """Numeric columns with low null ratio should get fill_median."""
    df = pd.DataFrame({"price": [1.0, 2.0, None, 4.0, 5.0]})
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)
    missing_sugg = [s for s in suggestions if s.finding.issue_type == "missing_values"]
    assert len(missing_sugg) == 1
    assert missing_sugg[0].params["strategy"] == "fill_median"


def test_missing_strategy_string_column() -> None:
    """String columns should get fill_mode, not fill_median."""
    df = pd.DataFrame({"city": ["Boston", "Austin", None, "Denver", "Boston"]})
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)
    missing_sugg = [s for s in suggestions if s.finding.issue_type == "missing_values"]
    assert len(missing_sugg) == 1
    assert missing_sugg[0].params["strategy"] == "fill_mode"


def test_missing_strategy_high_null_ratio_drops() -> None:
    """Columns with >50% nulls should get drop_rows."""
    df = pd.DataFrame({"val": [1.0, None, None, None, None]})
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)
    missing_sugg = [s for s in suggestions if s.finding.issue_type == "missing_values"]
    assert len(missing_sugg) == 1
    assert missing_sugg[0].params["strategy"] == "drop_rows"


# ============================================================================
# CONFLICT RESOLUTION TESTS - ensure exclusion rules prevent conflicts
# ============================================================================


def test_boolean_excludes_lowercase() -> None:
    """Boolean conversion should exclude lowercase suggestion for same column."""
    # Column with boolean-like values AND mixed case
    df = pd.DataFrame(
        {
            "active": ["Yes", "NO", "TRUE", "false", "yes"],
            "other": ["Hello", "WORLD", "Test", "hello", "world"],
        }
    )
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)

    # Should have boolean conversion for 'active'
    bool_suggs = [
        s
        for s in suggestions
        if s.transformer == "types" and s.params.get("target_type") == "boolean"
    ]
    assert len(bool_suggs) >= 1
    bool_cols = [c for s in bool_suggs for c in s.params.get("columns", [])]
    assert "active" in bool_cols

    # Should NOT have case standardization for 'active' (excluded by boolean)
    case_suggs = [
        s
        for s in suggestions
        if s.transformer == "formats"
        and s.params.get("operation") in ("lowercase", "uppercase", "titlecase")
    ]
    case_cols = [c for s in case_suggs for c in s.params.get("columns", [])]
    assert "active" not in case_cols


def test_date_excludes_lowercase() -> None:
    """Date standardization should exclude lowercase suggestion for same column."""
    # Use more date rows to ensure detection and trigger mixed case detection
    df = pd.DataFrame(
        {
            "date": [
                "15-Feb-2024",
                "2024/03/20",
                "Jan 5, 2024",
                "2024-01-01",
                "March 10, 2024",
                "2024/04/15",
                "Apr 20, 2024",
                "2024-05-25",
            ],
            "name": ["JOHN", "jane", "Bob", "ALICE", "MIKE", "sara", "Tom", "EVE"],
        }
    )
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)

    # Should have date standardization for 'date'
    date_suggs = [
        s
        for s in suggestions
        if s.transformer == "formats"
        and s.params.get("operation") == "standardize_dates"
    ]
    assert (
        len(date_suggs) >= 1
    ), f"No date suggestions found. Findings: {[f.issue_type for f in findings]}"
    date_cols = [c for s in date_suggs for c in s.params.get("columns", [])]
    assert "date" in date_cols

    # Should NOT have case standardization for 'date' (excluded by standardize_dates)
    case_suggs = [
        s
        for s in suggestions
        if s.transformer == "formats"
        and s.params.get("operation") in ("lowercase", "uppercase", "titlecase")
    ]
    case_cols = [c for s in case_suggs for c in s.params.get("columns", [])]
    assert "date" not in case_cols


def test_empty_strings_uses_clean_strategy() -> None:
    """Empty string issues should use clean_empty_strings strategy (combined convert+fill)."""
    df = pd.DataFrame(
        {
            "email": ["a@b.com", "", "c@d.com", "  ", "e@f.com"],
        }
    )
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)

    # Find empty string suggestion
    empty_suggs = [s for s in suggestions if s.finding.issue_type == "empty_strings"]
    assert len(empty_suggs) >= 1

    # Should use clean_empty_strings strategy (not empty_to_nan)
    for s in empty_suggs:
        assert s.params.get("strategy") == "clean_empty_strings"


def test_numeric_excludes_case_changes() -> None:
    """Numeric conversion should exclude case suggestions for same column."""
    df = pd.DataFrame(
        {
            "amount": ["100", "200.50", "300", "-50"],
            "status": ["ACTIVE", "inactive", "PENDING", "active"],
        }
    )
    profile = profile_dataset(df)
    findings = run_all_detectors(df, profile)
    suggestions = generate_suggestions(findings)

    # Should have numeric conversion for 'amount'
    num_suggs = [
        s
        for s in suggestions
        if s.transformer == "types" and s.params.get("target_type") == "numeric"
    ]
    assert len(num_suggs) >= 1
    num_cols = [c for s in num_suggs for c in s.params.get("columns", [])]
    assert "amount" in num_cols

    # Should NOT have case standardization for 'amount' (excluded by numeric)
    case_suggs = [
        s
        for s in suggestions
        if s.transformer == "formats"
        and s.params.get("operation") in ("lowercase", "uppercase", "titlecase")
    ]
    case_cols = [c for s in case_suggs for c in s.params.get("columns", [])]
    assert "amount" not in case_cols


def _suggest(df: pd.DataFrame, **kwargs: object) -> list:
    profile = profile_dataset(df)
    return generate_suggestions(run_all_detectors(df, profile), **kwargs)


def test_pii_column_is_never_filled() -> None:
    df = pd.DataFrame(
        {
            "email": ["a@b.com", None, "c@d.com", "d@e.com", "e@f.com"],
            "score": [1.0, None, 3.0, 4.0, 5.0],
        }
    )
    suggestions = _suggest(df)
    fills = {
        col
        for s in suggestions
        if s.transformer == "missing"
        for col in s.params["columns"]
    }
    assert "email" not in fills
    assert "score" in fills
    assert any(s.transformer == "pii" for s in suggestions)


def test_pii_empty_strings_become_missing_without_fill() -> None:
    df = pd.DataFrame({"email": ["a@b.com", "", "c@d.com", "  ", "e@f.com"]})
    empty = [s for s in _suggest(df) if s.finding.issue_type == "empty_strings"]
    assert empty
    assert all(s.params["fill_strategy"] == "none" for s in empty)


@pytest.mark.parametrize(
    "earlier",
    [
        ("formats", {"operation": "strip_whitespace"}),
        ("formats", {"operation": "lowercase"}),
        ("missing", {"strategy": "fill_mode"}),
        ("missing", {"strategy": "fill_median"}),
        ("missing", {"strategy": "clean_empty_strings"}),
    ],
)
@pytest.mark.parametrize("pii_op", ["mask", "hash", "redact", "generalize"])
def test_pii_runs_after_value_cleaning(earlier: tuple, pii_op: str) -> None:
    from datawash.suggestors.engine import _get_transform_order

    transformer, params = earlier
    assert _get_transform_order("pii", {"operation": pii_op}) > _get_transform_order(
        transformer, params
    )


@pytest.mark.parametrize(
    "later",
    [
        ("types", {"target_type": "boolean"}),
        ("types", {"target_type": "numeric"}),
        ("formats", {"operation": "standardize_dates"}),
        ("missing", {"strategy": "clip_outliers"}),
    ],
)
def test_pii_runs_before_type_conversion(later: tuple) -> None:
    """Masking str()s every value, so it must not undo typed columns."""
    from datawash.suggestors.engine import _get_transform_order

    transformer, params = later
    assert _get_transform_order("pii", {"operation": "mask"}) < _get_transform_order(
        transformer, params
    )


def test_name_like_column_is_still_filled_without_mask_names() -> None:
    df = pd.DataFrame({"product_name": ["Lamp", None, "Desk", "Lamp", "Chair"]})
    fills = [s for s in _suggest(df) if s.transformer == "missing"]
    assert [s.params["strategy"] for s in fills] == ["fill_mode"]
    with_names = _suggest(df, mask_names=True)
    assert not [s for s in with_names if s.transformer == "missing"]


@pytest.mark.parametrize("mask_names", [False, True])
def test_name_masking_is_opt_in(mask_names: bool) -> None:
    df = pd.DataFrame({"customer_name": ["Alice Smith", "Bob Jones", "Carol King"]})
    suggestions = _suggest(df, mask_names=mask_names)
    masked = [s for s in suggestions if s.transformer == "pii"]
    assert bool(masked) is mask_names
