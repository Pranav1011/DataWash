"""Base suggestor interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from datawash.core.models import Finding, Suggestion


class BaseSuggestor(ABC):
    """Maps findings to actionable suggestions."""

    @abstractmethod
    def suggest(self, finding: Finding) -> Suggestion | None:
        """Generate a suggestion for a finding, or None if not applicable."""
