"""Python code generation from transformation log."""

from __future__ import annotations

from datawash.core.models import TransformationResult


def generate_code(
    results: list[TransformationResult],
    style: str = "function",
    include_comments: bool = True,
) -> str:
    """Generate Python code from a list of transformation results.

    Args:
        results: List of TransformationResult from applied transformations.
        style: "function" wraps in a function, "script" generates standalone.
        include_comments: Whether to add explanatory comments.

    Returns:
        Python source code as a string.
    """
    if not results:
        return "# No transformations to apply"

    lines: list[str] = []

    # Header
    lines.append("import pandas as pd")
    lines.append("import numpy as np")
    lines.append("")

    if style == "function":
        lines.append("")
        lines.append("def clean_data(df: pd.DataFrame) -> pd.DataFrame:")
        lines.append('    """Apply data cleaning transformations."""')
        lines.append("    df = df.copy()")
        lines.append("")
        for result in results:
            if include_comments:
                lines.append(
                    f"    # {result.transformer}: {result.rows_affected} rows affected"
                )
            for code_line in result.code.split("\n"):
                if code_line.strip():
                    # Skip redundant imports inside function
                    if code_line.startswith("import "):
                        continue
                    lines.append(f"    {code_line}")
            lines.append("")
        lines.append("    return df")
    else:
        if include_comments:
            lines.append("# Load data")
        lines.append('df = pd.read_csv("input.csv")  # Update path as needed')
        lines.append("")
        for result in results:
            if include_comments:
                lines.append(
                    f"# {result.transformer}: {result.rows_affected} rows affected"
                )
            for code_line in result.code.split("\n"):
                if code_line.strip():
                    if code_line.startswith("import "):
                        continue
                    lines.append(code_line)
            lines.append("")
        if include_comments:
            lines.append("# Save cleaned data")
        lines.append('df.to_csv("output.csv", index=False)')

    return "\n".join(lines) + "\n"
