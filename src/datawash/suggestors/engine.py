"""Suggestion generation engine."""

from __future__ import annotations

import logging
from collections import defaultdict

from datawash.core.models import Finding, Severity, Suggestion
from datawash.suggestors.prioritizer import sort_suggestions

logger = logging.getLogger(__name__)


# Transformation execution order - later phases should not undo earlier phases
# The tuple is (transformer, operation/strategy) for precise matching
TRANSFORMATION_ORDER: list[tuple[str, str]] = [
    # Phase 1: Structural cleaning (affects row count)
    ("duplicates", "drop_duplicates"),
    ("fuzzy_duplicates", "remove"),  # ML-based fuzzy duplicate removal
    ("missing", "drop_rows"),
    # Phase 2: Value normalization (changes string values)
    ("formats", "strip_whitespace"),
    ("formats", "lowercase"),
    ("formats", "uppercase"),
    ("formats", "titlecase"),
    ("missing", "clean_empty_strings"),  # combined: empty→NaN→fill
    # Phase 3: Missing value handling (fills NaN)
    ("missing", "fill_mode"),
    ("missing", "fill_median"),
    ("missing", "fill_value"),
    ("missing", "empty_to_nan"),  # legacy, prefer clean_empty_strings
    # Phase 4: Type conversion (after all string cleaning done)
    ("types", "boolean"),
    ("types", "numeric"),
    ("formats", "standardize_dates"),
    # Phase 5: Outlier handling (after types are correct)
    ("missing", "clip_outliers"),
    # Phase 6: PII handling, after the values it masks are cleaned and filled
    ("pii", "redact"),
    ("pii", "mask"),
    ("pii", "hash"),
    ("pii", "generalize"),
    # Phase 7: Column operations (last)
    ("columns", "drop"),
    ("columns", "rename"),
    ("columns", "review_merge"),
]


def _get_transform_order(transformer: str, params: dict) -> int:
    """Get execution order for a transformation."""
    # Determine the operation/strategy key
    if transformer == "missing":
        key = params.get("strategy", "")
    elif transformer == "formats":
        key = params.get("operation", "")
    elif transformer == "types":
        key = params.get("target_type", "")
    elif transformer == "duplicates":
        key = "drop_duplicates"
    elif transformer == "fuzzy_duplicates":
        key = "remove"
    elif transformer == "columns":
        key = params.get("operation", "")
    elif transformer == "pii":
        key = params.get("operation", "")
    else:
        key = ""

    for i, (t, op) in enumerate(TRANSFORMATION_ORDER):
        if t == transformer and op == key:
            return i
    return 999  # Unknown transformations go last


# Exclusion rules: if a column has suggestion A, exclude suggestion B for same column
# Key: (transformer, operation/strategy), Value: list of (transformer, operation) to exclude
EXCLUSION_RULES: dict[tuple[str, str], list[tuple[str, str]]] = {
    # If column will be converted to boolean, don't suggest case changes
    ("types", "boolean"): [
        ("formats", "lowercase"),
        ("formats", "uppercase"),
        ("formats", "titlecase"),
    ],
    # If column will be converted to datetime, don't suggest case changes
    ("formats", "standardize_dates"): [
        ("formats", "lowercase"),
        ("formats", "uppercase"),
        ("formats", "titlecase"),
    ],
    # If column will be converted to numeric, don't suggest case changes
    ("types", "numeric"): [
        ("formats", "lowercase"),
        ("formats", "uppercase"),
        ("formats", "titlecase"),
    ],
}


def _get_transform_key(transformer: str, params: dict) -> tuple[str, str]:
    """Get the (transformer, operation) key for exclusion matching."""
    if transformer == "missing":
        return (transformer, params.get("strategy", ""))
    elif transformer == "formats":
        return (transformer, params.get("operation", ""))
    elif transformer == "types":
        return (transformer, params.get("target_type", ""))
    elif transformer == "duplicates":
        return (transformer, "drop_duplicates")
    elif transformer == "fuzzy_duplicates":
        return (transformer, "remove")
    elif transformer == "columns":
        return (transformer, params.get("operation", ""))
    elif transformer == "pii":
        return (transformer, params.get("operation", ""))
    return (transformer, "")


_FILL_STRATEGIES = frozenset({"fill_mode", "fill_median", "fill_value"})


def _missing_strategy(finding: Finding) -> str:
    """Choose fill strategy based on column dtype and null ratio."""
    if finding.details.get("null_ratio", 0) > 0.5:
        return "drop_rows"
    dtype = finding.details.get("dtype", "")
    # Check for numeric types
    if any(kw in dtype for kw in ("int", "float", "Int", "Float", "number")):
        return "fill_median"
    # Check for boolean types
    if "bool" in dtype.lower():
        return "fill_mode"
    # String/object/categorical → fill_mode
    return "fill_mode"


# Maps (issue_type) -> (action, transformer, param_builder, impact, rationale)
_SUGGESTION_MAP: dict[str, dict] = {
    "missing_values": {
        "action": "Handle missing values",
        "transformer": "missing",
        "params_fn": lambda f: {
            "columns": f.columns,
            "strategy": _missing_strategy(f),
        },
        "impact": "Removes or fills null values to prevent errors",
        "rationale": "Missing values cause errors in ML and analysis",
    },
    "empty_strings": {
        "action": "Clean empty strings",
        "transformer": "missing",
        # Use combined strategy that converts empty→NaN and fills in one step
        "params_fn": lambda f: {
            "columns": f.columns,
            "strategy": "clean_empty_strings",
        },
        "impact": "Converts empty strings to proper values",
        "rationale": "Empty strings are often unintentional missing values",
    },
    "duplicate_rows": {
        "action": "Remove duplicate rows",
        "transformer": "duplicates",
        "params_fn": lambda f: {"keep": "first"},
        "impact": "Removes redundant data that skews analysis",
        "rationale": "Exact duplicates inflate counts and bias statistics",
    },
    "fuzzy_duplicates": {
        "action": "Remove semantically similar rows",
        "transformer": "fuzzy_duplicates",
        "params_fn": lambda f: {
            "columns": f.details.get("columns_compared", f.columns),
            "threshold": f.details.get("threshold", 0.85),
            "strategy": "most_complete",
        },
        "impact": "Removes near-duplicate rows that skew analysis",
        "rationale": "Semantically similar rows (typos, abbreviations) cause data quality issues",
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
    "pii_detected": {
        "action": "Mask or redact PII",
        "transformer": "pii",
        "params_fn": lambda f: {
            "columns": f.columns,
            "operation": "mask",
            "pii_type": f.details.get("pii_type", "unknown"),
            "visible_chars": 4,
        },
        "impact": "Protects sensitive data and ensures compliance (GDPR, HIPAA, CCPA)",
        "rationale": "Column contains personally identifiable information",
    },
}


_USE_CASE_BOOSTS: dict[str, dict[str, float]] = {
    "ml": {
        "duplicate_rows": 1.5,
        "fuzzy_duplicates": 1.5,  # Critical for ML training data quality
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


# Column-name matches that mean "person's name"; masking these is opt-in
_NAME_PII_PATTERNS = frozenset({"name", "first_name", "last_name", "full_name"})


def _is_name_pii(finding: Finding) -> bool:
    """True for PII findings that only rest on a name-like column name."""
    return (
        finding.issue_type == "pii_detected"
        and finding.details.get("detection_method") == "column_name"
        and finding.details.get("matched_pattern") in _NAME_PII_PATTERNS
    )


def _deduplicate_pii_suggestions(suggestions: list[Suggestion]) -> list[Suggestion]:
    """Remove duplicate PII suggestions for the same column.

    When a column has PII detected by both pattern matching AND column name heuristics,
    two separate suggestions are generated. This removes the lower-priority duplicate,
    keeping only the highest-priority PII suggestion per column.
    """
    # Track best PII suggestion per column
    best_pii: dict[str, Suggestion] = {}

    for s in suggestions:
        if s.transformer != "pii":
            continue

        for col in s.params.get("columns", []):
            existing = best_pii.get(col)
            if existing is None:
                best_pii[col] = s
            else:
                # Keep the higher priority one (HIGH > MEDIUM > LOW)
                priority_order = {"high": 0, "medium": 1, "low": 2}
                existing_priority = priority_order.get(existing.priority.value, 999)
                new_priority = priority_order.get(s.priority.value, 999)
                if new_priority < existing_priority:
                    best_pii[col] = s

    # Build set of suggestions to keep
    keep_ids = {id(s) for s in best_pii.values()}

    # Return non-PII suggestions plus best PII per column
    result = []
    for s in suggestions:
        if s.transformer != "pii":
            result.append(s)
        elif id(s) in keep_ids:
            result.append(s)
        else:
            logger.debug(
                "Removing duplicate PII suggestion for column '%s' (pii_type=%s)",
                s.params.get("columns", ["?"])[0],
                s.params.get("pii_type", "unknown"),
            )

    return result


def _apply_exclusion_rules(suggestions: list[Suggestion]) -> list[Suggestion]:
    """Remove suggestions that conflict with higher-priority transformations."""
    # Build a map of column → list of (transform_key, suggestion)
    col_transforms: dict[str, list[tuple[tuple[str, str], Suggestion]]] = defaultdict(
        list
    )

    for s in suggestions:
        key = _get_transform_key(s.transformer, s.params)
        for col in s.params.get("columns", []):
            col_transforms[col].append((key, s))

    # Find suggestions to exclude
    excluded_ids: set[int] = set()

    for col, transforms in col_transforms.items():
        # Check each transform against exclusion rules
        for key, _s in transforms:
            if key in EXCLUSION_RULES:
                # This transform excludes certain others for the same column
                to_exclude = EXCLUSION_RULES[key]
                for other_key, other_s in transforms:
                    if other_key in to_exclude:
                        excluded_ids.add(id(other_s))
                        logger.debug(
                            "Excluding %s for column '%s' due to %s",
                            other_key,
                            col,
                            key,
                        )

    return [s for s in suggestions if id(s) not in excluded_ids]


def _sort_by_execution_order(suggestions: list[Suggestion]) -> list[Suggestion]:
    """Sort suggestions by transformation execution order."""
    return sorted(
        suggestions, key=lambda s: _get_transform_order(s.transformer, s.params)
    )


def generate_suggestions(
    findings: list[Finding],
    max_suggestions: int = 50,
    use_case: str = "general",
    mask_names: bool = False,
) -> list[Suggestion]:
    """Generate prioritized suggestions from findings.

    PII columns are never filled: a filled email or name would be another
    person's data. Masking columns that are PII only because of a name-like
    column name (e.g. 'customer_name') happens only when mask_names is True.
    """
    boosts = _USE_CASE_BOOSTS.get(use_case, {})
    pii_columns = {
        col for f in findings if f.issue_type == "pii_detected" for col in f.columns
    }
    suggestions: list[Suggestion] = []
    for finding in findings:
        mapping = _SUGGESTION_MAP.get(finding.issue_type)
        if mapping is None:
            logger.debug(
                "No suggestion mapping for: %s",
                finding.issue_type,
            )
            continue
        if _is_name_pii(finding) and not mask_names:
            continue
        params = mapping["params_fn"](finding)
        if pii_columns.intersection(finding.columns):
            if params.get("strategy") in _FILL_STRATEGIES:
                continue
            if params.get("strategy") == "clean_empty_strings":
                params["fill_strategy"] = "none"

        # Apply use-case priority boost
        priority = finding.severity
        boost = boosts.get(finding.issue_type, 1.0)
        if boost >= 1.4 and priority == Severity.LOW:
            priority = Severity.MEDIUM
        elif boost >= 1.3 and priority == Severity.MEDIUM:
            priority = Severity.HIGH

        action = mapping["action"]
        # Include column names in action text for column-specific suggestions
        if finding.columns and len(finding.columns) <= 3:
            col_str = ", ".join(f"'{c}'" for c in finding.columns)
            action = f"{action} in {col_str}"

        suggestion = Suggestion(
            id=0,
            finding=finding,
            action=action,
            transformer=mapping["transformer"],
            params=params,
            priority=priority,
            impact=mapping["impact"],
            rationale=mapping["rationale"],
        )
        suggestions.append(suggestion)

    # Step 1: Deduplicate PII suggestions for same column
    suggestions = _deduplicate_pii_suggestions(suggestions)

    # Step 2: Apply exclusion rules (remove conflicting suggestions)
    suggestions = _apply_exclusion_rules(suggestions)

    # Step 3: Sort by priority (for display)
    suggestions = sort_suggestions(suggestions)

    # Step 4: Assign IDs and limit
    return suggestions[:max_suggestions]
