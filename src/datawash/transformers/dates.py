"""Mixed-format date parsing shared by transformers and their generated code."""

from __future__ import annotations

import inspect

import pandas as pd


def _parse_mixed_dates(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Parse mixed-format dates, reading numeric d/m/y values per column.

    Returns the parsed datetimes and a mask of ambiguous values left unparsed.
    Numeric values are read day-first when some have a first part over 12 and
    month-first when some have a second part over 12. With no evidence, or
    evidence both ways, values that read differently either way are ambiguous.
    """
    parts = (
        values.astype("string")
        .str.extract(r"^\s*(\d{1,2})[/.-](\d{1,2})[/.-](\d{4}|\d{2})\s*$")
        .apply(pd.to_numeric)
    )
    first, second, year = parts[0], parts[1], parts[2]
    year = year.mask(year < 69, year + 2000).mask(
        (year >= 69) & (year < 100), year + 1900
    )
    numeric = first.notna().astype(bool)
    day_first = bool((first > 12).any())
    month_first = bool((second > 12).any())
    ambiguous = numeric & (first <= 12) & (second <= 12) & (first != second)
    ambiguous = ambiguous.fillna(False).astype(bool) & (day_first == month_first)
    read_day_first = ((first > 12) | (day_first and not month_first)).fillna(False)
    day = first.where(read_day_first, second)
    month = second.where(read_day_first, first)

    parsed = pd.to_datetime(values.where(~numeric), errors="coerce", format="mixed")
    rows = numeric & ~ambiguous
    if rows.any():
        components = pd.DataFrame({"year": year, "month": month, "day": day})[rows]
        parsed[rows] = pd.to_datetime(components.astype("int64"), errors="coerce")
    return parsed, ambiguous


PARSE_MIXED_DATES_SOURCE = inspect.getsource(_parse_mixed_dates)


def ambiguous_warning(column: str, count: int, action: str) -> str:
    """Describe ambiguous dates for a transformation result."""
    return (
        f"{count} ambiguous date(s) in '{column}' {action}: "
        "could be day-first or month-first"
    )
