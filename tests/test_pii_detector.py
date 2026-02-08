"""Tests for PII detector and transformer."""

from __future__ import annotations

import pandas as pd
import pytest

from datawash.detectors.pii_detector import PIIDetector
from datawash.profiler import profile_dataset
from datawash.transformers import run_transformer


class TestPIIDetector:
    """Tests for the PIIDetector class."""

    @pytest.fixture
    def detector(self) -> PIIDetector:
        return PIIDetector()

    def test_detect_email(self, detector: PIIDetector) -> None:
        """Should detect email addresses."""
        df = pd.DataFrame(
            {"contact": ["john@example.com", "jane@test.org", "not-an-email"]}
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        email_findings = [f for f in findings if f.details.get("pii_type") == "email"]
        assert len(email_findings) == 1
        assert email_findings[0].details["match_count"] == 2

    def test_detect_ssn(self, detector: PIIDetector) -> None:
        """Should detect Social Security Numbers."""
        df = pd.DataFrame({"id": ["123-45-6789", "987-65-4321", "not-ssn"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        ssn_findings = [f for f in findings if f.details.get("pii_type") == "ssn"]
        assert len(ssn_findings) == 1
        assert ssn_findings[0].details["match_count"] == 2

    def test_detect_phone(self, detector: PIIDetector) -> None:
        """Should detect US phone numbers."""
        df = pd.DataFrame(
            {
                "phone": [
                    "555-123-4567",
                    "(555) 987-6543",
                    "+1 555 111 2222",
                    "not-phone",
                ]
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_us"
        ]
        assert len(phone_findings) == 1
        assert phone_findings[0].details["match_count"] == 3

    def test_detect_credit_card_with_luhn(self, detector: PIIDetector) -> None:
        """Should detect credit cards and validate with Luhn."""
        df = pd.DataFrame(
            {
                "card": [
                    "4532015112830366",  # Valid Visa (passes Luhn)
                    "1234567890123456",  # Invalid (fails Luhn)
                    "not-a-card",
                ]
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        cc_findings = [
            f for f in findings if f.details.get("pii_type") == "credit_card"
        ]
        # Should only count the valid card
        assert len(cc_findings) == 1
        assert cc_findings[0].details["match_count"] == 1

    def test_detect_ipv4(self, detector: PIIDetector) -> None:
        """Should detect IPv4 addresses."""
        df = pd.DataFrame({"ip": ["192.168.1.1", "10.0.0.1", "not-an-ip"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        ip_findings = [f for f in findings if f.details.get("pii_type") == "ipv4"]
        assert len(ip_findings) == 1
        assert ip_findings[0].details["match_count"] == 2

    def test_detect_zip(self, detector: PIIDetector) -> None:
        """Should detect US ZIP codes."""
        df = pd.DataFrame(
            {"postal": ["12345", "12345-6789", "1234"]}  # Last one too short
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        zip_findings = [f for f in findings if f.details.get("pii_type") == "zip_us"]
        assert len(zip_findings) == 1
        assert zip_findings[0].details["match_count"] == 2

    def test_column_name_detection(self, detector: PIIDetector) -> None:
        """Should flag columns based on suspicious names."""
        df = pd.DataFrame(
            {
                "social_security_number": ["some-value"],
                "email_address": ["test@test.com"],
                "random_column": ["data"],
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        # Should find column name matches
        name_findings = [
            f for f in findings if f.details.get("detection_method") == "column_name"
        ]
        assert len(name_findings) >= 2  # ssn and email

    def test_no_false_positives(self, detector: PIIDetector) -> None:
        """Should not detect PII in clean business data."""
        df = pd.DataFrame(
            {
                "product_id": ["ABC-123", "DEF-456"],
                "quantity": [10, 20],
                "category": ["Electronics", "Books"],
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        # Should not find high-severity PII
        high_severity = [f for f in findings if f.severity.value == "high"]
        assert len(high_severity) == 0

    def test_luhn_valid(self, detector: PIIDetector) -> None:
        """Luhn algorithm should accept valid card numbers."""
        # Known valid test card numbers
        assert detector._luhn_check("4532015112830366") is True
        assert detector._luhn_check("5425233430109903") is True
        assert detector._luhn_check("4916338506082832") is True

    def test_luhn_invalid(self, detector: PIIDetector) -> None:
        """Luhn algorithm should reject invalid card numbers."""
        assert detector._luhn_check("1234567890123456") is False
        assert detector._luhn_check("1111111111111111") is False
        assert detector._luhn_check("123") is False  # Too short
        assert detector._luhn_check("abcd") is False  # Not digits


class TestPIITransformer:
    """Tests for the PIITransformer class."""

    def test_mask(self) -> None:
        """Should mask values keeping last N chars visible."""
        df = pd.DataFrame({"ssn": ["123-45-6789", "987-65-4321"]})
        result_df, result = run_transformer(
            "pii", df, operation="mask", columns=["ssn"], visible_chars=4
        )
        assert result_df["ssn"].iloc[0].endswith("6789")
        assert result_df["ssn"].iloc[0].startswith("*")
        assert result.rows_affected == 2

    def test_mask_short_value(self) -> None:
        """Should fully mask values shorter than visible_chars."""
        df = pd.DataFrame({"code": ["123", "ab"]})
        result_df, result = run_transformer(
            "pii", df, operation="mask", columns=["code"], visible_chars=4
        )
        assert result_df["code"].iloc[0] == "***"
        assert result_df["code"].iloc[1] == "**"

    def test_hash_deterministic(self) -> None:
        """Same input should produce same hash."""
        df = pd.DataFrame({"email": ["test@test.com", "test@test.com"]})
        result_df, result = run_transformer(
            "pii", df, operation="hash", columns=["email"]
        )
        assert result_df["email"].iloc[0] == result_df["email"].iloc[1]

    def test_hash_with_salt(self) -> None:
        """Different salts should produce different hashes."""
        df = pd.DataFrame({"email": ["test@test.com"]})
        result1, _ = run_transformer(
            "pii", df.copy(), operation="hash", columns=["email"], salt="salt1"
        )
        result2, _ = run_transformer(
            "pii", df.copy(), operation="hash", columns=["email"], salt="salt2"
        )
        assert result1["email"].iloc[0] != result2["email"].iloc[0]

    def test_redact(self) -> None:
        """Should replace values with placeholder."""
        df = pd.DataFrame({"ssn": ["123-45-6789", "987-65-4321"]})
        result_df, result = run_transformer(
            "pii", df, operation="redact", columns=["ssn"]
        )
        assert result_df["ssn"].iloc[0] == "[REDACTED]"
        assert result_df["ssn"].iloc[1] == "[REDACTED]"

    def test_redact_custom_replacement(self) -> None:
        """Should use custom replacement text."""
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        result_df, result = run_transformer(
            "pii", df, operation="redact", columns=["ssn"], replacement="***REMOVED***"
        )
        assert result_df["ssn"].iloc[0] == "***REMOVED***"

    def test_generalize_email(self) -> None:
        """Should generalize email to ****@domain."""
        df = pd.DataFrame({"email": ["john.doe@gmail.com", "jane@company.org"]})
        result_df, result = run_transformer(
            "pii", df, operation="generalize", columns=["email"], pii_type="email"
        )
        assert result_df["email"].iloc[0] == "****@gmail.com"
        assert result_df["email"].iloc[1] == "****@company.org"

    def test_generalize_phone(self) -> None:
        """Should generalize phone to keep area code only."""
        df = pd.DataFrame({"phone": ["555-123-4567", "(212) 555-1234"]})
        result_df, result = run_transformer(
            "pii", df, operation="generalize", columns=["phone"], pii_type="phone"
        )
        assert "555" in result_df["phone"].iloc[0]
        assert "***" in result_df["phone"].iloc[0]
        assert "212" in result_df["phone"].iloc[1]

    def test_generalize_zip(self) -> None:
        """Should truncate ZIP+4 to ZIP5."""
        df = pd.DataFrame({"zip": ["12345-6789", "90210"]})
        result_df, result = run_transformer(
            "pii", df, operation="generalize", columns=["zip"], pii_type="zip"
        )
        assert result_df["zip"].iloc[0] == "12345"
        assert result_df["zip"].iloc[1] == "90210"

    def test_handles_nulls(self) -> None:
        """Should preserve null values."""
        df = pd.DataFrame({"email": ["test@test.com", None, ""]})
        result_df, result = run_transformer(
            "pii", df, operation="mask", columns=["email"]
        )
        assert pd.isna(result_df["email"].iloc[1])

    def test_generate_code_mask(self) -> None:
        """Should generate mask code."""
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        _, result = run_transformer(
            "pii", df, operation="mask", columns=["ssn"], visible_chars=4
        )
        assert "_mask_pii" in result.code
        assert "visible=4" in result.code

    def test_generate_code_hash(self) -> None:
        """Should generate hash code."""
        df = pd.DataFrame({"email": ["test@test.com"]})
        _, result = run_transformer("pii", df, operation="hash", columns=["email"])
        assert "hashlib" in result.code
        assert "_hash_pii" in result.code

    def test_generate_code_redact(self) -> None:
        """Should generate redact code."""
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        _, result = run_transformer("pii", df, operation="redact", columns=["ssn"])
        assert "[REDACTED]" in result.code


class TestPIIIntegration:
    """Integration tests for PII detection and transformation."""

    def test_full_pipeline(self) -> None:
        """Test detection and transformation together."""
        from datawash import analyze

        df = pd.DataFrame(
            {
                "name": ["John Doe", "Jane Smith"],
                "email": ["john@example.com", "jane@test.org"],
                "ssn": ["123-45-6789", "987-65-4321"],
                "salary": [50000, 60000],
            }
        )

        report = analyze(df)

        # Should detect PII
        pii_issues = [f for f in report.issues if f.detector == "pii"]
        assert len(pii_issues) >= 2  # email and ssn at minimum

        # Should generate suggestions
        pii_suggestions = [s for s in report.suggestions if s.transformer == "pii"]
        assert len(pii_suggestions) >= 2

    def test_apply_pii_suggestions(self) -> None:
        """Test applying PII suggestions."""
        from datawash import analyze

        df = pd.DataFrame(
            {
                "email": ["john@example.com", "jane@test.org"],
                "ssn": ["123-45-6789", "987-65-4321"],
            }
        )

        report = analyze(df)

        # Find PII suggestions
        pii_suggestions = [s for s in report.suggestions if s.transformer == "pii"]
        if pii_suggestions:
            # Apply first suggestion
            clean_df = report.apply([pii_suggestions[0].id])
            # At least one column should be modified
            assert not clean_df.equals(df)


class TestPIIDetectorV031Fixes:
    """Tests for v0.3.1 PII detection improvements."""

    @pytest.fixture
    def detector(self) -> PIIDetector:
        return PIIDetector()

    # ==========================================================================
    # Fix 1: No false positives on numeric columns with PII-like names
    # ==========================================================================

    def test_no_false_positive_numeric_email_count(self, detector: PIIDetector) -> None:
        """email_count (integer) should NOT be flagged as PII."""
        df = pd.DataFrame(
            {
                "email_count": [5, 10, 15, 20],
                "phone_calls": [100, 200, 300, 400],
                "name_length": [8, 12, 6, 10],
                "address_id": [1001, 1002, 1003, 1004],
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        # Should have NO column name PII findings (all columns are numeric)
        name_findings = [
            f for f in findings if f.details.get("detection_method") == "column_name"
        ]
        assert (
            len(name_findings) == 0
        ), f"False positives: {[f.columns for f in name_findings]}"

    def test_string_column_with_pii_name_still_detected(
        self, detector: PIIDetector
    ) -> None:
        """String column named 'email' with actual emails should be detected."""
        df = pd.DataFrame(
            {
                "email": ["john@test.com", "jane@test.com"],
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        assert len(findings) >= 1
        # Should detect both by column name AND by pattern
        email_pattern = [f for f in findings if f.details.get("pii_type") == "email"]
        assert len(email_pattern) >= 1

    # ==========================================================================
    # Fix 2: SSN variations
    # ==========================================================================

    def test_ssn_with_dashes(self, detector: PIIDetector) -> None:
        """Standard SSN format (XXX-XX-XXXX) should be detected."""
        df = pd.DataFrame({"id": ["123-45-6789", "987-65-4321"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        ssn_findings = [f for f in findings if f.details.get("pii_type") == "ssn"]
        assert len(ssn_findings) == 1
        assert ssn_findings[0].details["match_count"] == 2

    def test_ssn_with_spaces(self, detector: PIIDetector) -> None:
        """SSN with spaces (XXX XX XXXX) should be detected."""
        df = pd.DataFrame({"tax_id": ["123 45 6789", "987 65 4321"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        ssn_findings = [f for f in findings if f.details.get("pii_type") == "ssn_space"]
        assert len(ssn_findings) == 1
        assert ssn_findings[0].details["match_count"] == 2

    def test_ssn_with_dots(self, detector: PIIDetector) -> None:
        """SSN with dots (XXX.XX.XXXX) should be detected."""
        df = pd.DataFrame({"ssn": ["123.45.6789", "987.65.4321"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        ssn_findings = [f for f in findings if f.details.get("pii_type") == "ssn_dot"]
        assert len(ssn_findings) == 1
        assert ssn_findings[0].details["match_count"] == 2

    def test_ssn_no_separator_with_context(self, detector: PIIDetector) -> None:
        """9-digit SSN without separator, with SSN-related column name."""
        df = pd.DataFrame({"social_security": ["123456789", "987654321"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        # Should detect because column name contains "social"
        ssn_findings = [f for f in findings if "ssn" in f.details.get("pii_type", "")]
        assert len(ssn_findings) >= 1

    def test_ssn_no_separator_without_context_no_false_positive(
        self, detector: PIIDetector
    ) -> None:
        """9-digit number without SSN context should NOT be flagged as SSN."""
        df = pd.DataFrame({"order_id": ["123456789", "987654321"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        # Should NOT flag as SSN (could be order ID, account number, etc.)
        ssn_nodash_findings = [
            f for f in findings if f.details.get("pii_type") == "ssn_nodash"
        ]
        assert len(ssn_nodash_findings) == 0

    # ==========================================================================
    # Fix 3: International phones
    # ==========================================================================

    def test_phone_uk(self, detector: PIIDetector) -> None:
        """UK phone numbers should be detected."""
        df = pd.DataFrame({"contact": ["+44 20 7123 4567", "+44 7911 123456"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_uk"
        ]
        assert len(phone_findings) >= 1

    def test_phone_germany(self, detector: PIIDetector) -> None:
        """German phone numbers should be detected."""
        df = pd.DataFrame({"contact": ["+49 30 12345678", "+49 89 1234567"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_eu"
        ]
        assert len(phone_findings) >= 1

    def test_phone_japan(self, detector: PIIDetector) -> None:
        """Japanese phone numbers should be detected."""
        df = pd.DataFrame({"contact": ["+81 3-1234-5678", "+81 90-1234-5678"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_intl"
        ]
        assert len(phone_findings) >= 1

    def test_phone_india(self, detector: PIIDetector) -> None:
        """Indian phone numbers should be detected."""
        df = pd.DataFrame({"contact": ["+91 98765 43210", "+91 11 2345 6789"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_intl"
        ]
        assert len(phone_findings) >= 1

    # ==========================================================================
    # Fix 4: Amex cards (15 digits)
    # ==========================================================================

    def test_credit_card_amex(self, detector: PIIDetector) -> None:
        """American Express cards (15 digits, starts with 34/37) should be detected."""
        df = pd.DataFrame(
            {
                "card": [
                    "378282246310005",  # Valid Amex test number
                    "371449635398431",  # Another valid Amex
                ]
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        cc_findings = [
            f for f in findings if f.details.get("pii_type") == "credit_card_amex"
        ]
        assert len(cc_findings) >= 1

    def test_credit_card_amex_formatted(self, detector: PIIDetector) -> None:
        """Amex with spaces/dashes should be detected."""
        df = pd.DataFrame(
            {
                "card": [
                    "3782 822463 10005",
                    "3714-496353-98431",
                ]
            }
        )
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        cc_findings = [
            f for f in findings if f.details.get("pii_type") == "credit_card_amex"
        ]
        assert len(cc_findings) >= 1

    def test_luhn_amex(self, detector: PIIDetector) -> None:
        """Luhn should validate 15-digit Amex cards."""
        # Valid Amex test numbers
        assert detector._luhn_check("378282246310005") is True
        assert detector._luhn_check("371449635398431") is True
        assert detector._luhn_check("340000000000009") is True

    # ==========================================================================
    # Regression tests - ensure existing functionality still works
    # ==========================================================================

    def test_regression_email_still_detected(self, detector: PIIDetector) -> None:
        """Emails should still be detected."""
        df = pd.DataFrame({"contact": ["john@example.com", "jane@test.org"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        email_findings = [f for f in findings if f.details.get("pii_type") == "email"]
        assert len(email_findings) >= 1

    def test_regression_us_phone_still_detected(self, detector: PIIDetector) -> None:
        """US phones should still be detected."""
        df = pd.DataFrame({"contact": ["555-123-4567", "(555) 987-6543"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        phone_findings = [
            f for f in findings if f.details.get("pii_type") == "phone_us"
        ]
        assert len(phone_findings) >= 1

    def test_regression_visa_still_detected(self, detector: PIIDetector) -> None:
        """16-digit Visa cards should still be detected."""
        df = pd.DataFrame({"card": ["4532015112830366"]})
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        cc_findings = [
            f for f in findings if f.details.get("pii_type") == "credit_card"
        ]
        assert len(cc_findings) >= 1

    def test_regression_luhn_still_rejects_invalid(self, detector: PIIDetector) -> None:
        """Luhn validation should still reject invalid cards."""
        df = pd.DataFrame({"card": ["1234567890123456"]})  # Invalid Luhn
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)
        cc_findings = [
            f for f in findings if f.details.get("pii_type") == "credit_card"
        ]
        # Should NOT detect (fails Luhn)
        assert len(cc_findings) == 0
