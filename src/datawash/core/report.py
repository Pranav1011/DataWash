"""Report class - main user-facing interface."""

from __future__ import annotations

import logging
import math
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from rich.console import Console
from rich.table import Table

from datawash.adapters import load_dataframe
from datawash.codegen import generate_code as _generate_code
from datawash.core.cache import ComputationCache
from datawash.core.config import Config
from datawash.core.dtypes import format_bytes, optimize_dataframe
from datawash.core.models import (
    DatasetProfile,
    Finding,
    Severity,
    Suggestion,
    TransformationResult,
)
from datawash.core.sampling import SAMPLE_THRESHOLD, SmartSampler
from datawash.detectors import run_all_detectors
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import get_all_detectors
from datawash.profiler import profile_dataset
from datawash.profiler.parallel import profile_dataset_parallel, run_detectors_parallel
from datawash.suggestors import generate_suggestions
from datawash.suggestors.engine import _sort_by_execution_order
from datawash.transformers import run_transformer

logger = logging.getLogger(__name__)

# Threshold below which parallel overhead isn't worth it
_PARALLEL_THRESHOLD = 5000

# Detectors that only run when ml_features=True
_ML_DETECTORS = frozenset({"fuzzy_duplicates"})


class Report:
    """Main interface for data analysis and cleaning.

    Args:
        data: A DataFrame or path to a data file.
        config: Optional configuration. Uses defaults if None.
        use_case: Context for suggestion prioritization.
        sample: Enable smart sampling for large datasets (default True).
        parallel: Enable parallel profiling and detection (default True).
        ml_features: Enable ML-powered features like semantic duplicate detection.
    """

    def __init__(
        self,
        data: pd.DataFrame | str | Path,
        config: Optional[Config | dict[str, Any]] = None,
        use_case: str = "general",
        sample: bool = True,
        parallel: bool = True,
        ml_features: bool = False,
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

        self._sample = sample
        self._parallel = parallel
        self._ml_features = ml_features

        self._sampler, self._profile, self._findings = self._analyze(self._df)

        # Restore original row/column counts in profile when sampled
        if self._sampler and self._sampler.is_sampled:
            self._profile = DatasetProfile(
                row_count=len(self._df),
                column_count=len(self._df.columns),
                memory_bytes=int(self._df.memory_usage(deep=True).sum()),
                columns=self._profile.columns,
                duplicate_row_count=self._sampler.extrapolate_count(
                    self._profile.duplicate_row_count
                ),
                sampled=True,
                sample_size=len(self._sampler.sample_df),
            )

        self._suggestions = generate_suggestions(
            self._findings,
            max_suggestions=self._config.suggestions.max_suggestions,
            use_case=self._config.use_case,
            mask_names=self._config.suggestions.mask_names,
        )
        # None until apply()/apply_interactive() runs; [] means nothing applied.
        self._applied: list[TransformationResult] | None = None
        self._last_score_before: int | None = None
        self._last_score_after: int | None = None

    def _active_detectors(self) -> dict[str, BaseDetector]:
        """Detectors enabled in config, plus ML detectors if ml_features is on."""
        enabled = self._config.detectors.enabled
        return {
            name: detector
            for name, detector in get_all_detectors().items()
            if (name in _ML_DETECTORS and self._ml_features)
            or (name not in _ML_DETECTORS and (enabled is None or name in enabled))
        }

    def _analyze(
        self, df: pd.DataFrame
    ) -> tuple[SmartSampler | None, DatasetProfile, list[Finding]]:
        """Profile and run detectors on df using this report's settings.

        Used for both the initial analysis and the after-apply score, so
        the two scores see the same dtypes, sampling and detectors.
        """
        try:
            optimized_df = optimize_dataframe(df)
        except Exception:
            logger.debug("Dtype optimization skipped", exc_info=True)
            optimized_df = df

        sampler: SmartSampler | None = None
        if self._sample and len(optimized_df) >= SAMPLE_THRESHOLD:
            sampler = SmartSampler(optimized_df)
            analysis_df = sampler.sample_df
        else:
            analysis_df = optimized_df

        use_parallel = self._parallel and (
            len(analysis_df) > _PARALLEL_THRESHOLD or len(analysis_df.columns) > 20
        )
        active = self._active_detectors()
        if use_parallel:
            cache = ComputationCache(analysis_df)
            profile = profile_dataset_parallel(analysis_df, cache=cache)
            findings = run_detectors_parallel(analysis_df, profile, active)
        else:
            profile = profile_dataset(analysis_df)
            findings = run_all_detectors(analysis_df, profile, enabled=list(active))
        return sampler, profile, findings

    @staticmethod
    def _score_findings(findings: list[Finding], row_count: int) -> int:
        """Score 0-100: start at 100, subtract a confidence-scaled penalty each."""
        if row_count == 0:
            return 100
        penalties = []
        for finding in findings:
            if finding.severity == Severity.HIGH:
                penalty = 10.0
            elif finding.severity == Severity.MEDIUM:
                penalty = 5.0
            else:
                penalty = 2.0
            penalties.append(penalty * finding.confidence)
        # fsum is order-independent, so parallel detector order can't shift it
        score = 100.0 - math.fsum(penalties)
        return max(0, min(100, int(score)))

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
        return self._score_findings(self._findings, self._profile.row_count)

    @property
    def last_score_before(self) -> int | None:
        """Quality score before the last apply, or None if never applied."""
        return self._last_score_before

    @property
    def last_score_after(self) -> int | None:
        """Quality score after the last apply, or None if never applied."""
        return self._last_score_after

    def suggest(self, use_case: Optional[str] = None) -> list[Suggestion]:
        """Get filtered suggestions, optionally for a specific use case."""
        # For now, return all suggestions. Use-case filtering is Phase 2.
        return list(self._suggestions)

    def _compute_quality_score(self, df: pd.DataFrame) -> int:
        """Compute quality score for an arbitrary DataFrame.

        Uses the same analysis path as quality_score, so applying no
        suggestions leaves the score unchanged.
        """
        _, profile, findings = self._analyze(df)
        return self._score_findings(findings, profile.row_count)

    def apply(self, suggestion_ids: list[int]) -> pd.DataFrame:
        """Apply selected suggestions by ID, return cleaned DataFrame."""
        score_before = self.quality_score
        result_df = self._df.copy()
        id_map = {s.id: s for s in self._suggestions}
        applied: list[TransformationResult] = []
        # Recorded up front so an error mid-apply leaves what actually ran.
        self._applied = applied
        self._last_score_before = None
        self._last_score_after = None

        # Collect and sort suggestions by execution order
        suggestions_to_apply = []
        for sid in suggestion_ids:
            suggestion = id_map.get(sid)
            if suggestion is None:
                logger.warning("Suggestion ID %d not found, skipping", sid)
                continue
            suggestions_to_apply.append(suggestion)

        # Sort by transformation execution order to prevent conflicts
        suggestions_to_apply = _sort_by_execution_order(suggestions_to_apply)

        for suggestion in suggestions_to_apply:
            result_df, tx_result = run_transformer(
                suggestion.transformer, result_df, **suggestion.params
            )
            applied.append(tx_result)
            logger.info(
                "Applied suggestion %d (%s): %d rows affected",
                suggestion.id,
                suggestion.action,
                tx_result.rows_affected,
            )

        score_after = (
            self._compute_quality_score(result_df) if applied else score_before
        )
        diff = score_after - score_before
        sign = "+" if diff >= 0 else ""
        logger.info(
            "Quality score: %d → %d (%s%d)", score_before, score_after, sign, diff
        )
        self._last_score_before = score_before
        self._last_score_after = score_after
        return result_df

    def apply_all(self) -> pd.DataFrame:
        """Apply all suggestions and return cleaned DataFrame."""
        return self.apply([s.id for s in self._suggestions])

    def apply_interactive(
        self, input_fn: Any = None, console: Optional[Console] = None
    ) -> pd.DataFrame:
        """Interactively apply suggestions with user prompts.

        Args:
            input_fn: Callable for getting user input (default: builtin input).
                      Useful for testing with monkeypatch.
            console: Optional Rich Console for output.
        """
        if input_fn is None:
            input_fn = input
        if console is None:
            console = Console()

        score_before = self.quality_score
        result_df = self._df.copy()
        applied: list[TransformationResult] = []
        # Recorded up front so an error mid-apply leaves what actually ran.
        self._applied = applied
        self._last_score_before = None
        self._last_score_after = None
        apply_all = False

        # Sort suggestions by execution order to prevent conflicts
        sorted_suggestions = _sort_by_execution_order(list(self._suggestions))

        for suggestion in sorted_suggestions:
            if not apply_all:
                table = Table(title=f"Suggestion #{suggestion.id}", show_lines=True)
                table.add_column("Field", style="bold")
                table.add_column("Value")
                table.add_row("Action", suggestion.action)
                table.add_row("Priority", suggestion.priority.value)
                table.add_row("Impact", suggestion.impact)
                table.add_row("Rationale", suggestion.rationale)
                cols_list = suggestion.params.get("columns", [])
                if cols_list:
                    table.add_row("Columns", ", ".join(cols_list))
                else:
                    table.add_row("Columns", "all")
                console.print(table)

                # Show preview of affected rows
                cols = suggestion.params.get("columns", [])
                valid_cols = [c for c in cols if c in result_df.columns]
                if valid_cols:
                    affected_preview = result_df[valid_cols].head(5)
                    console.print("[dim]Preview (first 5 rows):[/dim]")
                    console.print(affected_preview.to_string())

                raw_choice = input_fn(
                    "\n[a]pply / [s]kip / apply [A]ll / [q]uit: "
                ).strip()
                choice = raw_choice.lower()
                if choice == "q":
                    break
                elif choice == "s":
                    continue
                elif choice == "all" or raw_choice == "A":
                    apply_all = True
                elif choice not in ("a", "apply", ""):
                    continue

            result_df, tx_result = run_transformer(
                suggestion.transformer, result_df, **suggestion.params
            )
            applied.append(tx_result)
            console.print(
                f"[green]✓ Applied:[/green] {suggestion.action} "
                f"({tx_result.rows_affected} rows affected)"
            )

        score_after = (
            self._compute_quality_score(result_df) if applied else score_before
        )
        diff = score_after - score_before
        sign = "+" if diff >= 0 else ""
        msg = f"\n[bold]Quality score: {score_before} → {score_after} ({sign}{diff})[/bold]"
        console.print(msg)
        self._last_score_before = score_before
        self._last_score_after = score_after
        return result_df

    def generate_code(self, style: str = "function") -> str:
        """Generate Python code for the transformations applied last.

        If neither apply(), apply_all() nor apply_interactive() has been
        called yet, all suggestions are applied first. If one of them was
        called but applied nothing, the returned code contains no
        transformations and a warning is logged.
        """
        if self._applied is None:
            self.apply_all()
        applied = self._applied or []
        if not applied:
            logger.warning(
                "No transformations were applied; generated code is a no-op"
            )
        return _generate_code(
            applied,
            style=style,
            include_comments=self._config.codegen.include_comments,
        )

    def summary(self) -> str:
        """Human-readable analysis summary."""
        lines = [
            f"Dataset: {self._profile.row_count} rows "
            f"x {self._profile.column_count} columns",
            f"Memory: {format_bytes(self._profile.memory_bytes)}",
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
    sample: bool = True,
    parallel: bool = True,
    ml_features: bool = False,
) -> Report:
    """Analyze a dataset and return a Report.

    This is the main entry point for datawash.

    Args:
        data: DataFrame or path to a data file.
        config: Optional configuration dict or Config object.
        use_case: One of "general", "ml", "analytics", "export".
        sample: Enable smart sampling for large datasets.
        parallel: Enable parallel profiling and detection.
        ml_features: Enable ML-powered features like semantic duplicate detection.
            Requires ML dependencies: pip install datawash[ml]

    Returns:
        A Report object with issues, suggestions, and cleaning methods.
    """
    return Report(
        data,
        config=config,
        use_case=use_case,
        sample=sample,
        parallel=parallel,
        ml_features=ml_features,
    )
