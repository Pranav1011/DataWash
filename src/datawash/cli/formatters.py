"""Rich output formatting for CLI."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from datawash.core.models import DatasetProfile, Finding, Suggestion

console = Console()

SEVERITY_COLORS = {"high": "red", "medium": "yellow", "low": "green"}


def format_profile(profile: DatasetProfile) -> None:
    """Print dataset profile."""
    console.print(
        Panel(
            f"[bold]{profile.row_count}[/] rows x "
            f"[bold]{profile.column_count}[/] columns | "
            f"Memory: [bold]"
            f"{profile.memory_bytes / 1024 / 1024:.1f} MB[/] | "
            f"Dupes: [bold]{profile.duplicate_row_count}[/]",
            title="Dataset Overview",
        )
    )

    table = Table(title="Column Profiles")
    table.add_column("Column", style="cyan")
    table.add_column("Type")
    table.add_column("Nulls", justify="right")
    table.add_column("Unique", justify="right")
    table.add_column("Sample Values")

    for name, col in profile.columns.items():
        null_str = (
            f"{col.null_count} ({col.null_ratio:.0%})" if col.null_count > 0 else "0"
        )
        samples = ", ".join(str(v) for v in col.sample_values[:3])
        semantic = f" [{col.semantic_type}]" if col.semantic_type else ""
        table.add_row(
            name, f"{col.dtype}{semantic}", null_str, str(col.unique_count), samples
        )

    console.print(table)


def format_issues(findings: list[Finding]) -> None:
    """Print detected issues."""
    if not findings:
        console.print("[green]No issues detected![/]")
        return

    table = Table(title=f"Issues Found ({len(findings)})")
    table.add_column("Severity", justify="center")
    table.add_column("Detector")
    table.add_column("Message")
    table.add_column("Columns")

    for f in findings:
        color = SEVERITY_COLORS.get(f.severity.value, "white")
        table.add_row(
            Text(f.severity.value.upper(), style=color),
            f.detector,
            f.message,
            ", ".join(f.columns),
        )

    console.print(table)


def format_suggestions(suggestions: list[Suggestion]) -> None:
    """Print suggestions."""
    if not suggestions:
        console.print("[green]No suggestions.[/]")
        return

    table = Table(title=f"Suggestions ({len(suggestions)})")
    table.add_column("#", justify="right", style="bold")
    table.add_column("Priority", justify="center")
    table.add_column("Action")
    table.add_column("Impact")
    table.add_column("Rationale")

    for s in suggestions:
        color = SEVERITY_COLORS.get(s.priority.value, "white")
        table.add_row(
            str(s.id),
            Text(s.priority.value.upper(), style=color),
            s.action,
            s.impact,
            s.rationale,
        )

    console.print(table)


def format_transformation_summary(
    before_rows: int, after_rows: int, n_applied: int
) -> None:
    """Print transformation summary."""
    console.print(
        Panel(
            f"Applied [bold]{n_applied}[/] transformation(s)\n"
            f"Rows: {before_rows} → {after_rows} ({before_rows - after_rows} removed)",
            title="Cleaning Summary",
        )
    )
