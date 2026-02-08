"""Detect personally identifiable information (PII) in datasets."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class PIIDetector(BaseDetector):
    """Detect personally identifiable information in datasets.

    Detects PII through:
    1. Regex pattern matching (email, SSN, phone, credit card, IP, ZIP)
    2. Column name heuristics (columns named 'ssn', 'email', etc.)

    v0.3.1 improvements:
    - Column name heuristics only apply to string columns (not numeric)
    - SSN detection supports variations (dashes, spaces, dots, no separator)
    - International phone number detection (UK, EU, generic)
    - American Express credit card detection (15 digits)
    """

    # PII patterns with their types and severity
    PII_PATTERNS: dict[str, dict[str, Any]] = {
        # Email
        "email": {
            "pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            "severity": Severity.HIGH,
            "description": "email addresses",
        },
        # SSN variations
        "ssn": {
            "pattern": r"\b\d{3}-\d{2}-\d{4}\b",
            "severity": Severity.HIGH,
            "description": "Social Security Numbers",
        },
        "ssn_space": {
            "pattern": r"\b\d{3}\s\d{2}\s\d{4}\b",
            "severity": Severity.HIGH,
            "description": "Social Security Numbers (spaced)",
        },
        "ssn_dot": {
            "pattern": r"\b\d{3}\.\d{2}\.\d{4}\b",
            "severity": Severity.HIGH,
            "description": "Social Security Numbers (dotted)",
        },
        "ssn_nodash": {
            "pattern": r"(?<!\d)\d{9}(?!\d)",
            "severity": Severity.HIGH,
            "description": "Social Security Numbers (no separator)",
            "requires_context": True,  # Only match if column name suggests SSN
        },
        # US Phone
        "phone_us": {
            "pattern": r"(?:\+1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
            "severity": Severity.HIGH,
            "description": "US phone numbers",
        },
        # International phones
        "phone_uk": {
            "pattern": r"\+44[-.\s]?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{4}",
            "severity": Severity.HIGH,
            "description": "UK phone numbers",
        },
        "phone_eu": {
            "pattern": r"\+(?:49|33|39|34|31)[-.\s]?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{2,4}",
            "severity": Severity.HIGH,
            "description": "EU phone numbers",
        },
        "phone_intl": {
            "pattern": r"\+(?:81|91|86|82|61|55|52)[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
            "severity": Severity.MEDIUM,
            "description": "International phone numbers",
        },
        # Credit cards
        "credit_card": {
            "pattern": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
            "severity": Severity.HIGH,
            "description": "credit card numbers",
            "validator": "luhn",
        },
        "credit_card_amex": {
            "pattern": r"\b3[47]\d{2}[-\s]?\d{6}[-\s]?\d{5}\b",
            "severity": Severity.HIGH,
            "description": "American Express card numbers",
            "validator": "luhn",
        },
        # Network
        "ipv4": {
            "pattern": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
            "severity": Severity.MEDIUM,
            "description": "IPv4 addresses",
        },
        # Location
        "zip_us": {
            "pattern": r"\b\d{5}(?:-\d{4})?\b",
            "severity": Severity.LOW,
            "description": "US ZIP codes",
        },
    }

    # Column names that suggest PII content
    PII_COLUMN_NAMES: dict[str, list[str]] = {
        "high": ["ssn", "social_security", "credit_card", "cc_number", "card_number"],
        "medium": ["email", "phone", "mobile", "cell", "address", "street"],
        "low": [
            "name",
            "first_name",
            "last_name",
            "full_name",
            "dob",
            "birth_date",
            "zip",
            "postal",
        ],
    }

    # Keywords that indicate SSN context (for 9-digit number detection)
    SSN_CONTEXT_KEYWORDS: list[str] = [
        "ssn",
        "social",
        "security",
        "tax",
        "tin",
        "itin",
        "ein",
    ]

    @property
    def name(self) -> str:
        return "pii"

    @property
    def description(self) -> str:
        return "Detects personally identifiable information (PII)"

    def detect(self, df: pd.DataFrame, profile: DatasetProfile) -> list[Finding]:
        """Detect PII in the dataset."""
        findings: list[Finding] = []

        for col in df.columns:
            col_lower = col.lower().replace(" ", "_").replace("-", "_")

            # Check column dtype
            col_dtype = df[col].dtype
            is_string_like = col_dtype is object or pd.api.types.is_string_dtype(
                col_dtype
            )
            is_numeric = pd.api.types.is_numeric_dtype(col_dtype)

            # FIX v0.3.1: Only apply column name heuristics to STRING columns
            # Numeric columns like "email_count" should NOT be flagged
            if is_string_like and not is_numeric:
                name_finding = self._check_column_name(col, col_lower)
                if name_finding:
                    findings.append(name_finding)

            # Check values for PII patterns (only for string-like columns)
            if is_string_like:
                pattern_findings = self._scan_for_patterns(df[col], col, col_lower)
                findings.extend(pattern_findings)

        return findings

    def _check_column_name(self, col: str, col_lower: str) -> Finding | None:
        """Check if column name suggests PII content."""
        for severity_str, names in self.PII_COLUMN_NAMES.items():
            for pii_name in names:
                if pii_name in col_lower:
                    severity = Severity[severity_str.upper()]
                    return Finding(
                        detector=self.name,
                        issue_type="pii_detected",
                        severity=severity,
                        columns=[col],
                        details={
                            "pii_type": "column_name",
                            "matched_pattern": pii_name,
                            "detection_method": "column_name",
                        },
                        message=(
                            f"Column '{col}' name suggests it may contain PII "
                            f"(matched: '{pii_name}')"
                        ),
                        confidence=0.7,
                    )
        return None

    def _has_ssn_context(self, col_lower: str) -> bool:
        """Check if column name suggests SSN content."""
        return any(kw in col_lower for kw in self.SSN_CONTEXT_KEYWORDS)

    def _scan_for_patterns(
        self, series: pd.Series, col: str, col_lower: str
    ) -> list[Finding]:
        """Scan column values for PII patterns."""
        findings: list[Finding] = []

        # Sample for large columns (first 1000 non-null values)
        sample = series.dropna().astype(str).head(1000)
        if len(sample) == 0:
            return findings

        for pii_type, config in self.PII_PATTERNS.items():
            pattern = config["pattern"]

            # Skip context-required patterns if context is missing
            if config.get("requires_context"):
                if pii_type.startswith("ssn") and not self._has_ssn_context(col_lower):
                    continue

            try:
                matches = sample.str.contains(pattern, regex=True, na=False)
                match_count = int(matches.sum())

                # Apply validator if specified (e.g., Luhn for credit cards)
                if match_count > 0 and config.get("validator") == "luhn":
                    matched_values = sample[matches]
                    match_count = self._validate_luhn_batch(matched_values)

                if match_count > 0:
                    match_ratio = match_count / len(sample)
                    findings.append(
                        Finding(
                            detector=self.name,
                            issue_type="pii_detected",
                            severity=config["severity"],
                            columns=[col],
                            details={
                                "pii_type": pii_type,
                                "match_count": match_count,
                                "match_ratio": round(match_ratio, 4),
                                "detection_method": "pattern",
                            },
                            message=(
                                f"Column '{col}' contains {match_count} potential "
                                f"{config['description']} ({match_ratio:.0%} of "
                                f"non-null values)"
                            ),
                            confidence=0.9 if match_ratio > 0.5 else 0.7,
                        )
                    )
            except re.error:
                # Skip invalid patterns
                continue

        return findings

    def _validate_luhn_batch(self, series: pd.Series) -> int:
        """Validate credit card numbers using Luhn algorithm."""
        valid_count = 0
        for val in series:
            digits = "".join(filter(str.isdigit, str(val)))
            if self._luhn_check(digits):
                valid_count += 1
        return valid_count

    @staticmethod
    def _luhn_check(card_number: str) -> bool:
        """Validate credit card number using Luhn algorithm.

        The Luhn algorithm (mod 10) is used to validate credit card numbers.
        Supports both 15-digit (Amex) and 16-digit (Visa/MC) cards.
        """
        if not card_number.isdigit() or len(card_number) < 13:
            return False

        total = 0
        reverse_digits = card_number[::-1]
        for i, digit in enumerate(reverse_digits):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n

        return total % 10 == 0


register_detector(PIIDetector())
