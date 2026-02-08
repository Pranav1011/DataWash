"""Transform PII data through masking, hashing, or redaction."""

from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

from datawash.core.models import TransformationResult
from datawash.transformers.base import BaseTransformer
from datawash.transformers.registry import register_transformer


class PIITransformer(BaseTransformer):
    """Transform PII data through masking, hashing, or redaction.

    Supported operations:
    - mask: Keep last N characters visible, mask the rest
    - hash: Replace with cryptographic hash (SHA256)
    - redact: Replace with placeholder text
    - generalize: Reduce specificity (e.g., email domain only)
    """

    @property
    def name(self) -> str:
        return "pii"

    def transform(
        self, df: pd.DataFrame, **params: Any
    ) -> tuple[pd.DataFrame, TransformationResult]:
        """Apply PII transformation."""
        operation = params.get("operation", "mask")
        columns = params.get("columns", [])
        result_df = df.copy()
        affected = 0

        for col in columns:
            if col not in result_df.columns:
                continue

            before_count = result_df[col].notna().sum()

            if operation == "mask":
                visible_chars = params.get("visible_chars", 4)
                mask_char = params.get("mask_char", "*")
                result_df[col] = result_df[col].apply(
                    lambda x: (
                        self._mask_value(x, visible_chars, mask_char)
                        if pd.notna(x)
                        else x
                    )
                )
                affected += int(before_count)

            elif operation == "hash":
                algorithm = params.get("algorithm", "sha256")
                salt = params.get("salt", "")
                result_df[col] = result_df[col].apply(
                    lambda x: self._hash_value(x, algorithm, salt) if pd.notna(x) else x
                )
                affected += int(before_count)

            elif operation == "redact":
                replacement = params.get("replacement", "[REDACTED]")
                result_df[col] = result_df[col].apply(
                    lambda x: replacement if pd.notna(x) and str(x).strip() else x
                )
                affected += int(before_count)

            elif operation == "generalize":
                pii_type = params.get("pii_type", "unknown")
                result_df[col] = result_df[col].apply(
                    lambda x: self._generalize_value(x, pii_type) if pd.notna(x) else x
                )
                affected += int(before_count)

        return result_df, TransformationResult(
            transformer=self.name,
            params=params,
            rows_affected=affected,
            columns_affected=columns,
            code=self.generate_code(**params),
        )

    def generate_code(self, **params: Any) -> str:
        """Generate equivalent pandas code."""
        operation = params.get("operation", "mask")
        columns = params.get("columns", [])
        lines: list[str] = []

        if operation == "mask":
            visible_chars = params.get("visible_chars", 4)
            mask_char = params.get("mask_char", "*")
            lines.append(
                f"def _mask_pii(val, visible={visible_chars}, char={repr(mask_char)}):"
            )
            lines.append("    if pd.isna(val): return val")
            lines.append("    val = str(val)")
            lines.append("    if len(val) <= visible: return char * len(val)")
            lines.append("    return char * (len(val) - visible) + val[-visible:]")
            lines.append("")
            for col in columns:
                lines.append(f"df[{repr(col)}] = df[{repr(col)}].apply(_mask_pii)")

        elif operation == "hash":
            algorithm = params.get("algorithm", "sha256")
            salt = params.get("salt", "")
            lines.append("import hashlib")
            lines.append("")
            lines.append(f"def _hash_pii(val, salt={repr(salt)}):")
            lines.append("    if pd.isna(val): return val")
            lines.append(f"    h = hashlib.{algorithm}()")
            lines.append("    h.update(f'{salt}{val}'.encode())")
            lines.append("    return h.hexdigest()[:16]")
            lines.append("")
            for col in columns:
                lines.append(f"df[{repr(col)}] = df[{repr(col)}].apply(_hash_pii)")

        elif operation == "redact":
            replacement = params.get("replacement", "[REDACTED]")
            for col in columns:
                lines.append(
                    f"df[{repr(col)}] = df[{repr(col)}].apply("
                    f"lambda x: {repr(replacement)} if pd.notna(x) and str(x).strip() else x)"
                )

        elif operation == "generalize":
            pii_type = params.get("pii_type", "unknown")
            if pii_type == "email":
                lines.append("def _generalize_email(val):")
                lines.append("    if pd.isna(val) or '@' not in str(val): return val")
                lines.append("    _, domain = str(val).rsplit('@', 1)")
                lines.append("    return f'****@{domain}'")
                lines.append("")
                for col in columns:
                    lines.append(
                        f"df[{repr(col)}] = df[{repr(col)}].apply(_generalize_email)"
                    )
            elif pii_type == "phone" or pii_type == "phone_us":
                lines.append("def _generalize_phone(val):")
                lines.append("    if pd.isna(val): return val")
                lines.append("    digits = ''.join(filter(str.isdigit, str(val)))")
                lines.append(
                    "    if len(digits) >= 10: return f'{digits[:3]}-***-****'"
                )
                lines.append("    return '***-***-****'")
                lines.append("")
                for col in columns:
                    lines.append(
                        f"df[{repr(col)}] = df[{repr(col)}].apply(_generalize_phone)"
                    )
            elif pii_type == "zip" or pii_type == "zip_us":
                lines.append("def _generalize_zip(val):")
                lines.append("    if pd.isna(val): return val")
                lines.append("    digits = ''.join(filter(str.isdigit, str(val)))")
                lines.append("    return digits[:5] if len(digits) >= 5 else digits")
                lines.append("")
                for col in columns:
                    lines.append(
                        f"df[{repr(col)}] = df[{repr(col)}].apply(_generalize_zip)"
                    )
            else:
                # Default: mask
                for col in columns:
                    lines.append(
                        f"df[{repr(col)}] = df[{repr(col)}].apply("
                        "lambda x: '****' + str(x)[-4:] if pd.notna(x) else x)"
                    )

        return "\n".join(lines) if lines else "# No PII transformation applied"

    @staticmethod
    def _mask_value(value: Any, visible_chars: int, mask_char: str) -> str:
        """Mask a value, keeping last N characters visible."""
        value = str(value)
        if len(value) <= visible_chars:
            return mask_char * len(value)
        return mask_char * (len(value) - visible_chars) + value[-visible_chars:]

    @staticmethod
    def _hash_value(value: Any, algorithm: str, salt: str) -> str:
        """Hash a value using the specified algorithm."""
        hasher = hashlib.new(algorithm)
        hasher.update(f"{salt}{value}".encode())
        return hasher.hexdigest()[:16]  # Truncate for readability

    @staticmethod
    def _generalize_value(value: Any, pii_type: str) -> str:
        """Generalize a value to reduce identifiability."""
        if pd.isna(value):
            return value

        value_str = str(value)

        if pii_type == "email":
            if "@" not in value_str:
                return value_str
            _, domain = value_str.rsplit("@", 1)
            return f"****@{domain}"

        elif pii_type in ("phone", "phone_us"):
            digits = "".join(filter(str.isdigit, value_str))
            if len(digits) >= 10:
                return f"{digits[:3]}-***-****"
            return "***-***-****"

        elif pii_type in ("zip", "zip_us"):
            digits = "".join(filter(str.isdigit, value_str))
            return digits[:5] if len(digits) >= 5 else digits

        else:
            # Default: mask last 4 chars visible
            if len(value_str) <= 4:
                return "*" * len(value_str)
            return "*" * (len(value_str) - 4) + value_str[-4:]


register_transformer(PIITransformer())
