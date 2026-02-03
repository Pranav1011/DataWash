"""Suggestion prioritization."""

from __future__ import annotations

from datawash.core.models import Severity, Suggestion

SEVERITY_WEIGHTS = {Severity.HIGH: 3, Severity.MEDIUM: 2, Severity.LOW: 1}


def priority_score(suggestion: Suggestion) -> float:
    """Compute a numeric priority score for sorting."""
    severity_val = SEVERITY_WEIGHTS.get(suggestion.priority, 1)
    confidence = suggestion.finding.confidence
    # Impact approximated from severity
    return severity_val * 0.5 + confidence * 0.5


def sort_suggestions(suggestions: list[Suggestion]) -> list[Suggestion]:
    """Sort suggestions by priority score descending, reassign IDs."""
    ranked = sorted(suggestions, key=priority_score, reverse=True)
    for i, s in enumerate(ranked, 1):
        s.id = i
    return ranked
