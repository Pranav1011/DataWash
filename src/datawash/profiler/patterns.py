"""Pattern detection for common data formats."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"^[\w.+-]+@[\w-]+\.[\w.-]+$"),
    "url": re.compile(r"^https?://[\w\-._~:/?#\[\]@!$&'()*+,;=]+$"),
    "phone": re.compile(r"^[\+]?[(]?[0-9]{1,4}[)]?[-\s./0-9]{6,15}$"),
    "ipv4": re.compile(r"^(?:\d{1,3}\.){3}\d{1,3}$"),
    "uuid": re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
    ),
    "currency": re.compile("^[$\u20ac\u00a3\u00a5][\\s]?[\\d,]+\\.?\\d*$"),
    "zip_us": re.compile(r"^\d{5}(-\d{4})?$"),
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%d-%m-%Y",
    "%Y%m%d",
    "%b %d, %Y",
    "%B %d, %Y",
    "%d %b %Y",
    "%d %B %Y",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]


def detect_column_patterns(series: pd.Series) -> dict[str, Any]:
    """Detect patterns in a column's values.

    Returns dict with detected pattern name and match ratio.
    """
    clean = series.dropna().astype(str)
    if clean.empty:
        return {}

    sample = clean.head(1000)
    total = len(sample)
    results: dict[str, Any] = {}

    for name, pattern in PATTERNS.items():
        matches = sample.str.match(pattern).sum()
        ratio = matches / total
        if ratio > 0.5:
            results[name] = {
                "match_ratio": round(float(ratio), 3),
                "pattern": pattern.pattern,
            }

    # Date detection
    if not results:
        date_ratio = _detect_date_pattern(sample)
        if date_ratio and date_ratio["match_ratio"] > 0.5:
            results["date"] = date_ratio

    return results


def _detect_date_pattern(sample: pd.Series) -> dict[str, Any] | None:
    """Try parsing dates with common formats."""
    for fmt in DATE_FORMATS:
        try:
            parsed = pd.to_datetime(sample, format=fmt, errors="coerce")
            ratio = parsed.notna().sum() / len(sample)
            if ratio > 0.7:
                return {"match_ratio": round(float(ratio), 3), "format": fmt}
        except Exception:
            continue
    return None
