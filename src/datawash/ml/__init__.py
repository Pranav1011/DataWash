"""Machine Learning utilities for DataWash.

This module provides ML-powered data quality features:
- Semantic duplicate detection using sentence embeddings
- (Future) ML-based anomaly detection
- (Future) Intelligent type inference

Install ML dependencies with: pip install datawash[ml]
"""

from __future__ import annotations


def _check_ml_dependencies() -> bool:
    """Check if ML dependencies are installed."""
    try:
        import sentence_transformers  # noqa: F401

        return True
    except ImportError:
        return False


ML_AVAILABLE = _check_ml_dependencies()


def require_ml() -> None:
    """Raise ImportError if ML dependencies are not installed."""
    if not ML_AVAILABLE:
        raise ImportError(
            "ML features require additional dependencies. "
            "Install with: pip install datawash[ml]"
        )


# Conditional exports
if ML_AVAILABLE:
    from datawash.ml.embeddings import TextEmbedder as TextEmbedder
    from datawash.ml.fuzzy_duplicates import (
        FuzzyDuplicateFinder as FuzzyDuplicateFinder,
    )
    from datawash.ml.similarity import SimilaritySearch as SimilaritySearch

    __all__ = [
        "TextEmbedder",
        "SimilaritySearch",
        "FuzzyDuplicateFinder",
        "ML_AVAILABLE",
        "require_ml",
    ]
else:
    __all__ = ["ML_AVAILABLE", "require_ml"]
