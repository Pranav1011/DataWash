"""Suggestion generation engine."""

from __future__ import annotations

import logging

from datawash.core.models import Finding, Severity, Suggestion
from datawash.suggestors.prioritizer import sort_suggestions

logger = logging.getLogger(__name__)

# Maps (issue_type) -> (action, transformer, param_builder, impact, rationale)
_SUGGESTION_MAP: dict[str, dict] = {
    "missing_values": {
        "action": "Handle missing values",
        "transformer": "missing",
        "params_fn": lambda f: {
            "columns": f.columns,
            "strategy": (
                "drop_rows" if f.details.get("null_ratio", 0) > 0.5 else "fill_median"
            ),
        },
        "impact": "Removes or fills null values to prevent errors",
        "rationale": "Missing values cause errors in ML and analysis",
    },
    "empty_strings": {
        "action": "Convert empty strings to NaN",
        "transformer": "missing",
        "params_fn": lambda f: {"columns": f.columns, "strategy": "empty_to_nan"},
        "impact": "Standardizes missing value representation",
        "rationale": "Empty strings are often unintentional missing values",
    },
    "duplicate_rows": {
        "action": "Remove duplicate rows",
        "transformer": "duplicates",
        "params_fn": lambda f: {"keep": "first"},
        "impact": "Removes redundant data that skews analysis",
        "rationale": "Exact duplicates inflate counts and bias statistics",
    },
    "inconsistent_case": {
        "action": "Standardize text casing",
        "transformer": "formats",
        "params_fn": lambda f: {"columns": f.columns, "operation": "lowercase"},
        "impact": "Ensures consistent text representation",
        "rationale": "Mixed casing causes mismatches in grouping and joins",
    },
    "inconsistent_date_format": {
        "action": "Standardize date format",
        "transformer": "formats",
        "params_fn": lambda f: {
            "columns": f.columns,
            "operation": "standardize_dates",
            "target_format": "%Y-%m-%d",
        },
        "impact": "Ensures consistent date parsing",
        "rationale": "Mixed date formats cause parsing errors",
    },
    "whitespace_padding": {
        "action": "Strip whitespace from values",
        "transformer": "formats",
        "params_fn": lambda f: {"columns": f.columns, "operation": "strip_whitespace"},
        "impact": "Removes accidental padding that causes mismatches",
        "rationale": "Leading/trailing whitespace causes silent matching failures",
    },
    "outliers": {
        "action": "Review and handle outliers",
        "transformer": "missing",
        "params_fn": lambda f: {
            "columns": f.columns,
            "strategy": "clip_outliers",
            "method": f.details.get("method", "iqr"),
            "threshold": f.details.get("threshold", 1.5),
        },
        "impact": "Reduces influence of extreme values on analysis",
        "rationale": "Outliers can heavily skew means and model training",
    },
    "numeric_as_string": {
        "action": "Convert to numeric type",
        "transformer": "types",
        "params_fn": lambda f: {"columns": f.columns, "target_type": "numeric"},
        "impact": "Enables numeric operations and reduces memory",
        "rationale": "Numeric data stored as strings prevents mathematical operations",
    },
    "boolean_as_string": {
        "action": "Convert to boolean type",
        "transformer": "types",
        "params_fn": lambda f: {"columns": f.columns, "target_type": "boolean"},
        "impact": "Correct type enables boolean operations",
        "rationale": "Boolean data as strings wastes memory and prevents logic ops",
    },
    "similar_columns": {
        "action": "Review potentially duplicate columns",
        "transformer": "columns",
        "params_fn": lambda f: {"columns": f.columns, "operation": "review_merge"},
        "impact": "May reduce redundant data",
        "rationale": "Similar columns may be duplicated data or candidates for merging",
    },
}


_USE_CASE_BOOSTS: dict[str, dict[str, float]] = {
    "ml": {
        "duplicate_rows": 1.5,
        "missing_values": 1.3,
        "numeric_as_string": 1.3,
        "boolean_as_string": 1.2,
        "outliers": 1.2,
        "similar_columns": 1.4,
    },
    "analytics": {
        "missing_values": 1.5,
        "outliers": 1.3,
        "inconsistent_date_format": 1.4,
        "inconsistent_case": 1.2,
    },
    "export": {
        "inconsistent_date_format": 1.5,
        "whitespace_padding": 1.4,
        "inconsistent_case": 1.3,
        "numeric_as_string": 1.3,
    },
    "general": {},
}


def generate_suggestions(
    findings: list[Finding],
    max_suggestions: int = 50,
    use_case: str = "general",
) -> list[Suggestion]:
    """Generate prioritized suggestions from findings."""
    boosts = _USE_CASE_BOOSTS.get(use_case, {})
    suggestions: list[Suggestion] = []
    for finding in findings:
        mapping = _SUGGESTION_MAP.get(finding.issue_type)
        if mapping is None:
            logger.debug(
                "No suggestion mapping for: %s",
                finding.issue_type,
            )
            continue

        # Apply use-case priority boost
        priority = finding.severity
        boost = boosts.get(finding.issue_type, 1.0)
        if boost >= 1.4 and priority == Severity.LOW:
            priority = Severity.MEDIUM
        elif boost >= 1.3 and priority == Severity.MEDIUM:
            priority = Severity.HIGH

        suggestion = Suggestion(
            id=0,
            finding=finding,
            action=mapping["action"],
            transformer=mapping["transformer"],
            params=mapping["params_fn"](finding),
            priority=priority,
            impact=mapping["impact"],
            rationale=mapping["rationale"],
        )
        suggestions.append(suggestion)

    suggestions = sort_suggestions(suggestions)
    return suggestions[:max_suggestions]
