"""CLI entry point for datawash."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(
    name="datawash",
    help="Intelligent data cleaning and quality analysis.",
    no_args_is_help=True,
)
console = Console()


@app.command()
def analyze(
    file: Path = typer.Argument(..., help="Path to data file"),
    sample: Optional[int] = typer.Option(
        None, "--sample", "-s", help="Number of rows to sample"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output"),
) -> None:
    """Analyze a dataset and show quality report."""
    from datawash.cli.formatters import (
        format_issues,
        format_profile,
        format_suggestions,
    )
    from datawash.core.report import Report

    with console.status("Analyzing..."):
        report = Report(str(file))

    format_profile(report.profile)
    console.print()
    format_issues(report.issues)
    console.print()
    format_suggestions(report.suggestions)


@app.command()
def suggest(
    file: Path = typer.Argument(..., help="Path to data file"),
    use_case: str = typer.Option(
        "general", "--use-case", "-u", help="Context: general, ml, analytics, export"
    ),
    priority: str = typer.Option(
        "all", "--priority", "-p", help="Filter: high, medium, low, all"
    ),
    limit: int = typer.Option(20, "--limit", "-l", help="Max suggestions"),
) -> None:
    """Show cleaning suggestions for a dataset."""
    from datawash.cli.formatters import format_suggestions
    from datawash.core.report import Report

    with console.status("Analyzing..."):
        report = Report(str(file), use_case=use_case)

    suggestions = report.suggestions
    if priority != "all":
        suggestions = [s for s in suggestions if s.priority.value == priority]
    suggestions = suggestions[:limit]

    format_suggestions(suggestions)


@app.command()
def clean(
    file: Path = typer.Argument(..., help="Path to data file"),
    output: Path = typer.Option(..., "--output", "-o", help="Output file path"),
    apply: Optional[str] = typer.Option(
        None, "--apply", "-a", help="Suggestion IDs (comma-separated)"
    ),
    apply_all: bool = typer.Option(False, "--apply-all", help="Apply all suggestions"),
    use_case: str = typer.Option("general", "--use-case", "-u", help="Context"),
    codegen: Optional[Path] = typer.Option(
        None, "--codegen", help="Also save generated Python code"
    ),
) -> None:
    """Clean a dataset by applying suggestions."""
    from datawash.adapters.base import _ADAPTERS
    from datawash.cli.formatters import format_transformation_summary
    from datawash.core.report import Report

    with console.status("Analyzing..."):
        report = Report(str(file), use_case=use_case)

    before_rows = len(report.df)

    if apply_all:
        clean_df = report.apply_all()
    elif apply:
        ids = [int(x.strip()) for x in apply.split(",")]
        clean_df = report.apply(ids)
    else:
        console.print("[red]Specify --apply or --apply-all[/]")
        raise typer.Exit(1)

    # Save output
    ext = output.suffix.lstrip(".")
    adapter = _ADAPTERS.get(ext)
    if adapter is None:
        console.print(f"[red]Unsupported output format: {ext}[/]")
        raise typer.Exit(1)
    adapter.write(clean_df, output)
    console.print(f"Saved cleaned data to [bold]{output}[/]")

    format_transformation_summary(before_rows, len(clean_df), len(report._applied))

    # Show before/after quality score
    score_before = getattr(report, "_last_score_before", None)
    score_after = getattr(report, "_last_score_after", None)
    if score_before is not None and score_after is not None:
        diff = score_after - score_before
        sign = "+" if diff >= 0 else ""
        console.print(f"Quality score: {score_before} → {score_after} ({sign}{diff})")

    if codegen:
        code = report.generate_code()
        codegen.write_text(code)
        console.print(f"Saved code to [bold]{codegen}[/]")


@app.command()
def codegen(
    file: Path = typer.Argument(..., help="Path to data file"),
    apply: Optional[str] = typer.Option(
        None, "--apply", "-a", help="Suggestion IDs (comma-separated)"
    ),
    apply_all: bool = typer.Option(
        False, "--apply-all", help="Generate code for all suggestions"
    ),
    style: str = typer.Option(
        "function", "--style", "-s", help="Code style: function or script"
    ),
    output: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Output Python file"
    ),
) -> None:
    """Generate Python code for data cleaning transformations."""
    from datawash.core.report import Report

    with console.status("Analyzing..."):
        report = Report(str(file))

    if apply_all:
        report.apply_all()
    elif apply:
        ids = [int(x.strip()) for x in apply.split(",")]
        report.apply(ids)
    else:
        report.apply_all()

    code = report.generate_code(style=style)

    if output:
        output.write_text(code)
        console.print(f"Saved code to [bold]{output}[/]")
    else:
        console.print(code)


if __name__ == "__main__":
    app()
