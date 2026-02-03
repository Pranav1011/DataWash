"""Report class - main user-facing interface."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from datawash.adapters import load_dataframe
from datawash.codegen import generate_code as _generate_code
from datawash.core.config import Config
from datawash.core.models import (
    DatasetProfile,
    Finding,
    Severity,
    Suggestion,
    TransformationResult,
)
from datawash.detectors import run_all_detectors
from datawash.profiler import profile_dataset
from datawash.suggestors import generate_suggestions
from datawash.transformers import run_transformer

logger = logging.getLogger(__name__)


class Report:
    """Main interface for data analysis and cleaning.

    Args:
        data: A DataFrame or path to a data file.
        config: Optional configuration. Uses defaults if None.
        use_case: Context for suggestion prioritization.
    """

    def __init__(
        self,
        data: pd.DataFrame | str | Path,
        config: Optional[Config | dict[str, Any]] = None,
        use_case: str = "general",
    ) -> None:
        # Resolve config
        if config is None:
            self._config = Config(use_case=use_case)
        elif isinstance(config, dict):
            config.setdefault("use_case", use_case)
            self._config = Config.from_dict(config)
        else:
            self._config = config

        # Load data
        if isinstance(data, (str, Path)):
            self._df = load_dataframe(data)
            self._source_path = str(data)
        else:
            self._df = data
            self._source_path = None

        # Run analysis
        self._profile = profile_dataset(self._df)
        self._findings = run_all_detectors(
            self._df,
            self._profile,
            enabled=self._config.detectors.enabled,
        )
        self._suggestions = generate_suggestions(
            self._findings,
            max_suggestions=self._config.suggestions.max_suggestions,
            use_case=self._config.use_case,
        )
        self._applied: list[TransformationResult] = []

    @property
    def df(self) -> pd.DataFrame:
        """The original DataFrame (read-only copy)."""
        return self._df.copy()

    @property
    def profile(self) -> DatasetProfile:
        """Dataset profile with statistics."""
        return self._profile

    @property
    def issues(self) -> list[Finding]:
        """All detected data quality issues."""
        return list(self._findings)

    @property
    def suggestions(self) -> list[Suggestion]:
        """Prioritized list of suggestions."""
        return list(self._suggestions)

    @property
    def quality_score(self) -> int:
        """Data quality score from 0 to 100."""
        score = 100.0
        if self._profile.row_count == 0:
            return 100
        for finding in self._findings:
            if finding.severity == Severity.HIGH:
                penalty = 10.0
            elif finding.severity == Severity.MEDIUM:
                penalty = 5.0
            else:
                penalty = 2.0
            # Scale by confidence
            penalty *= finding.confidence
            score -= penalty
        return max(0, min(100, int(score)))

    def suggest(self, use_case: Optional[str] = None) -> list[Suggestion]:
        """Get filtered suggestions, optionally for a specific use case."""
        # For now, return all suggestions. Use-case filtering is Phase 2.
        return list(self._suggestions)

    def apply(self, suggestion_ids: list[int]) -> pd.DataFrame:
        """Apply selected suggestions by ID, return cleaned DataFrame."""
        result_df = self._df.copy()
        id_map = {s.id: s for s in self._suggestions}
        self._applied = []

        for sid in suggestion_ids:
            suggestion = id_map.get(sid)
            if suggestion is None:
                logger.warning("Suggestion ID %d not found, skipping", sid)
                continue
            result_df, tx_result = run_transformer(
                suggestion.transformer, result_df, **suggestion.params
            )
            self._applied.append(tx_result)
            logger.info(
                "Applied suggestion %d (%s): %d rows affected",
                sid,
                suggestion.action,
                tx_result.rows_affected,
            )

        return result_df

    def apply_all(self) -> pd.DataFrame:
        """Apply all suggestions and return cleaned DataFrame."""
        return self.apply([s.id for s in self._suggestions])

    def generate_code(self, style: str = "function") -> str:
        """Generate Python code for applied transformations.

        Must call apply() or apply_all() first.
        """
        if not self._applied:
            # Auto-apply all if nothing applied yet
            self.apply_all()
        return _generate_code(
            self._applied,
            style=style,
            include_comments=self._config.codegen.include_comments,
        )

    def summary(self) -> str:
        """Human-readable analysis summary."""
        lines = [
            f"Dataset: {self._profile.row_count} rows "
            f"x {self._profile.column_count} columns",
            f"Memory: {self._profile.memory_bytes / 1024 / 1024:.1f} MB",
            f"Duplicate rows: {self._profile.duplicate_row_count}",
            f"Data Quality Score: {self.quality_score}/100",
            f"Issues found: {len(self._findings)}",
            f"Suggestions: {len(self._suggestions)}",
            "",
        ]

        # Group issues by severity
        for severity in ["high", "medium", "low"]:
            issues = [f for f in self._findings if f.severity.value == severity]
            if issues:
                lines.append(f"  [{severity.upper()}] {len(issues)} issue(s)")
                for issue in issues[:5]:
                    lines.append(f"    - {issue.message}")
                if len(issues) > 5:
                    lines.append(f"    ... and {len(issues) - 5} more")

        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"Report(rows={self._profile.row_count}, "
            f"cols={self._profile.column_count}, "
            f"issues={len(self._findings)}, "
            f"suggestions={len(self._suggestions)})"
        )

    def _repr_html_(self) -> str:
        """Rich display for Jupyter notebooks."""
        html = [
            "<div style='font-family: monospace;'>",
            "<h3>DataWash Report</h3>",
            f"<p><b>Dataset:</b> {self._profile.row_count} rows "
            f"x {self._profile.column_count} columns</p>",
            f"<p><b>Issues:</b> {len(self._findings)} | "
            f"<b>Suggestions:</b> {len(self._suggestions)}</p>",
        ]
        if self._suggestions:
            html.append("<table border='1' style='border-collapse: collapse;'>")
            html.append(
                "<tr><th>#</th><th>Priority</th><th>Action</th><th>Impact</th></tr>"
            )
            for s in self._suggestions[:10]:
                color = {"high": "red", "medium": "orange", "low": "green"}.get(
                    s.priority.value, "gray"
                )
                html.append(
                    f"<tr><td>{s.id}</td>"
                    f"<td style='color:{color};'>{s.priority.value}</td>"
                    f"<td>{s.action}</td>"
                    f"<td>{s.impact}</td></tr>"
                )
            if len(self._suggestions) > 10:
                html.append(
                    f"<tr><td colspan='4'>... and {len(self._suggestions) - 10} more</td></tr>"
                )
            html.append("</table>")
        html.append("</div>")
        return "\n".join(html)


def analyze(
    data: pd.DataFrame | str | Path,
    config: Optional[Config | dict[str, Any]] = None,
    use_case: str = "general",
) -> Report:
    """Analyze a dataset and return a Report.

    This is the main entry point for datawash.

    Args:
        data: DataFrame or path to a data file.
        config: Optional configuration dict or Config object.
        use_case: One of "general", "ml", "analytics", "export".

    Returns:
        A Report object with issues, suggestions, and cleaning methods.
    """
    return Report(data, config=config, use_case=use_case)
