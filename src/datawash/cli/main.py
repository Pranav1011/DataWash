"""CLI entry point for datawash."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, Optional

import typer
from rich.console import Console
from rich.markup import escape

from datawash.core.exceptions import AdapterError

if TYPE_CHECKING:
    from datawash.core.report import Report

app = typer.Typer(
    name="datawash",
    help="Intelligent data cleaning and quality analysis.",
    no_args_is_help=True,
)
console = Console()


def _fail(message: str) -> NoReturn:
    """Print a one-line user error in red and exit with code 1."""
    console.print(f"[red]{escape(message)}[/]", soft_wrap=True)
    raise typer.Exit(1)


def _load_report(file: Path, **kwargs: Any) -> Report:
    """Analyze a data file, turning load errors into a clean CLI error."""
    from datawash.core.report import Report

    try:
        with console.status("Analyzing..."):
            return Report(str(file), **kwargs)
    except AdapterError as e:
        _fail(str(e))


def _parse_ids(apply: str) -> list[int]:
    """Parse a comma-separated --apply value into suggestion IDs."""
    try:
        return [int(x.strip()) for x in apply.split(",")]
    except ValueError:
        _fail(
            f"Invalid --apply value '{apply}'. Use comma-separated suggestion "
            "numbers from 'datawash suggest', e.g. --apply 1,3"
        )


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

    report = _load_report(file)

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

    report = _load_report(file, use_case=use_case)

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

    report = _load_report(file, use_case=use_case)

    before_rows = len(report.df)

    if apply_all:
        clean_df = report.apply_all()
    elif apply:
        ids = _parse_ids(apply)
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

    n_applied = len(report._applied or [])
    format_transformation_summary(before_rows, len(clean_df), n_applied)

    # Show before/after quality score
    score_before = report.last_score_before
    score_after = report.last_score_after
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
    report = _load_report(file)

    if apply_all:
        report.apply_all()
    elif apply:
        ids = _parse_ids(apply)
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
