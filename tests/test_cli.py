"""Tests for CLI commands."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from datawash.cli.main import app

runner = CliRunner()


@pytest.fixture
def csv_file(tmp_path: Path, messy_df: pd.DataFrame) -> Path:
    path = tmp_path / "test.csv"
    messy_df.to_csv(path, index=False)
    return path


def test_analyze_command(csv_file: Path) -> None:
    result = runner.invoke(app, ["analyze", str(csv_file)])
    assert result.exit_code == 0
    assert "Dataset Overview" in result.output or "Column Profiles" in result.output


def test_suggest_command(csv_file: Path) -> None:
    result = runner.invoke(app, ["suggest", str(csv_file)])
    assert result.exit_code == 0


def test_suggest_with_priority(csv_file: Path) -> None:
    result = runner.invoke(app, ["suggest", str(csv_file), "--priority", "high"])
    assert result.exit_code == 0


def test_clean_command(csv_file: Path, tmp_path: Path) -> None:
    output = tmp_path / "clean.csv"
    result = runner.invoke(
        app, ["clean", str(csv_file), "-o", str(output), "--apply-all"]
    )
    assert result.exit_code == 0
    assert output.exists()


def test_clean_with_ids(csv_file: Path, tmp_path: Path) -> None:
    output = tmp_path / "clean.csv"
    result = runner.invoke(
        app, ["clean", str(csv_file), "-o", str(output), "--apply", "1,2"]
    )
    assert result.exit_code == 0


def test_clean_no_apply(csv_file: Path, tmp_path: Path) -> None:
    output = tmp_path / "clean.csv"
    result = runner.invoke(app, ["clean", str(csv_file), "-o", str(output)])
    assert result.exit_code == 1


def test_codegen_command(csv_file: Path) -> None:
    result = runner.invoke(app, ["codegen", str(csv_file), "--apply-all"])
    assert result.exit_code == 0
    assert "import pandas" in result.output


def test_codegen_to_file(csv_file: Path, tmp_path: Path) -> None:
    output = tmp_path / "clean.py"
    result = runner.invoke(
        app,
        ["codegen", str(csv_file), "--apply-all", "-o", str(output)],
    )
    assert result.exit_code == 0
    assert output.exists()


def test_codegen_script_style(csv_file: Path) -> None:
    result = runner.invoke(
        app, ["codegen", str(csv_file), "--apply-all", "--style", "script"]
    )
    assert result.exit_code == 0
    assert "read_csv" in result.output


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "datawash" in result.output.lower() or "intelligent" in result.output.lower()
