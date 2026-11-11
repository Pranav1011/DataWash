"""Tests for CLI commands."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from datawash.cli.main import app
from datawash.codegen import generate_code

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


def test_clean_unknown_id_codegen_is_noop(csv_file: Path, tmp_path: Path) -> None:
    """Codegen reflects what was applied, even when nothing was."""
    output = tmp_path / "clean.csv"
    code_path = tmp_path / "clean.py"
    result = runner.invoke(
        app,
        [
            "clean",
            str(csv_file),
            "-o",
            str(output),
            "--apply",
            "999",
            "--codegen",
            str(code_path),
        ],
    )
    assert result.exit_code == 0
    assert code_path.read_text() == generate_code([])
    assert pd.read_csv(output).equals(pd.read_csv(csv_file))


MIXED_DATES_CSV = (
    "name,signup_date\n"
    "Alice,2024-01-15\n"
    "Bob,01/20/2024\n"
    "Carol,2024-01-25\n"
    'Dave,"Jan 30, 2024"\n'
    "Eve,2024-02-01\n"
)


def test_clean_standardizes_mixed_dates_and_codegen_matches(tmp_path: Path) -> None:
    src = tmp_path / "mixed.csv"
    src.write_text(MIXED_DATES_CSV)
    output = tmp_path / "clean.csv"
    code_path = tmp_path / "clean.py"
    result = runner.invoke(
        app,
        [
            "clean",
            str(src),
            "-o",
            str(output),
            "--apply-all",
            "--codegen",
            str(code_path),
        ],
    )
    assert result.exit_code == 0
    cleaned = pd.read_csv(output)
    assert cleaned["signup_date"].tolist() == [
        "2024-01-15",
        "2024-01-20",
        "2024-01-25",
        "2024-01-30",
        "2024-02-01",
    ]

    namespace: dict[str, object] = {}
    exec(code_path.read_text(), namespace)
    replayed = tmp_path / "replayed.csv"
    namespace["clean_data"](pd.read_csv(src)).to_csv(replayed, index=False)
    pd.testing.assert_frame_equal(pd.read_csv(replayed), cleaned)


def _write_png(path: Path) -> None:
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + bytes(range(256)))


@pytest.mark.parametrize(
    ("name", "setup", "expected"),
    [
        ("empty.csv", lambda p: p.write_text(""), "File is empty"),
        ("missing.csv", lambda p: None, "File not found"),
        ("image.png", _write_png, "Unsupported format: 'png'"),
        ("notes.txt", lambda p: p.write_text("hello"), "Unsupported format: 'txt'"),
        ("binary.csv", _write_png, "is not UTF-8 text"),
    ],
)
@pytest.mark.parametrize("command", ["analyze", "suggest", "clean", "codegen"])
def test_bad_input_file_gives_clean_error(
    tmp_path: Path, command: str, name: str, setup: object, expected: str
) -> None:
    path = tmp_path / name
    setup(path)  # type: ignore[operator]
    args = [command, str(path)]
    if command == "clean":
        args += ["-o", str(tmp_path / "out.csv"), "--apply-all"]
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert expected in result.output
    assert len(result.output.strip().splitlines()) == 1


@pytest.mark.parametrize("command", ["clean", "codegen"])
def test_non_numeric_apply_gives_clean_error(
    csv_file: Path, tmp_path: Path, command: str
) -> None:
    args = [command, str(csv_file), "--apply", "abc"]
    if command == "clean":
        args += ["-o", str(tmp_path / "out.csv")]
    result = runner.invoke(app, args)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert "Invalid --apply value 'abc'" in result.output
    assert len(result.output.strip().splitlines()) == 1


def test_unexpected_errors_still_surface(
    csv_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("bug")

    monkeypatch.setattr("datawash.core.report.Report.__init__", boom)
    result = runner.invoke(app, ["analyze", str(csv_file)])
    assert isinstance(result.exception, RuntimeError)


def test_clean_warns_about_ambiguous_dates(tmp_path: Path) -> None:
    src = tmp_path / "ambiguous.csv"
    src.write_text(
        "name,signup_date\n"
        "A,13/04/2024\n"
        "B,01/20/2024\n"
        "C,03/04/2024\n"
        "D,2024-02-01\n"
        "E,2024-02-02\n"
    )
    output = tmp_path / "clean.csv"
    result = runner.invoke(app, ["clean", str(src), "-o", str(output), "--apply-all"])
    assert result.exit_code == 0
    assert "1 ambiguous date(s) in 'signup_date' left unchanged" in result.output
    assert "03/04/2024" in pd.read_csv(output)["signup_date"].tolist()
