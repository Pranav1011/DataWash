"""Datawash - Intelligent data quality analysis and cleaning."""

from __future__ import annotations

from datawash.core.report import analyze

__version__ = "1.1.0"


def _check_ml_available() -> bool:
    """Check if ML features are available."""
    try:
        from datawash.ml import ML_AVAILABLE

        return ML_AVAILABLE
    except ImportError:
        return False


ml_available = _check_ml_available()

__all__ = ["analyze", "__version__", "ml_available"]
