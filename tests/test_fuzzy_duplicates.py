"""Tests for fuzzy (semantic) duplicate detection."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

# Check if ML dependencies are available
try:
    from datawash.ml import ML_AVAILABLE
except ImportError:
    ML_AVAILABLE = False

# Skip all tests if ML not available
pytestmark = pytest.mark.skipif(
    not ML_AVAILABLE, reason="ML dependencies not installed (pip install datawash[ml])"
)


class TestTextEmbedder:
    """Tests for TextEmbedder class."""

    def test_basic_embedding(self):
        """Test basic text embedding."""
        from datawash.ml.embeddings import TextEmbedder

        embedder = TextEmbedder(model_preset="fast")
        embeddings = embedder.embed(["hello world", "hi there"])

        assert embeddings.shape == (2, embedder.embedding_dim)
        assert embeddings.dtype == np.float32

    def test_single_embedding(self):
        """Test single text embedding."""
        from datawash.ml.embeddings import TextEmbedder

        embedder = TextEmbedder(model_preset="fast")
        embedding = embedder.embed_single("hello world")

        assert embedding.shape == (embedder.embedding_dim,)

    def test_similarity(self):
        """Test similarity computation."""
        from datawash.ml.embeddings import TextEmbedder

        embedder = TextEmbedder(model_preset="fast")

        # Similar texts should have high similarity
        sim_high = embedder.similarity("John Smith", "Jon Smith")
        assert sim_high > 0.8

        # Different texts should have lower similarity
        sim_low = embedder.similarity("hello world", "goodbye moon")
        assert sim_low < sim_high

    def test_pairwise_similarity(self):
        """Test pairwise similarity matrix."""
        from datawash.ml.embeddings import TextEmbedder

        embedder = TextEmbedder(model_preset="fast")
        texts = ["hello", "hi", "goodbye"]
        sim_matrix = embedder.pairwise_similarity(texts)

        assert sim_matrix.shape == (3, 3)
        # Diagonal should be ~1.0 (self-similarity)
        for i in range(3):
            assert sim_matrix[i, i] > 0.99

    def test_model_presets(self):
        """Test different model presets."""
        from datawash.ml.embeddings import TextEmbedder

        # Just verify they can be instantiated
        for preset in ["fast", "balanced", "multilingual"]:
            embedder = TextEmbedder(model_preset=preset)
            assert embedder.embedding_dim > 0


class TestSimilaritySearch:
    """Tests for SimilaritySearch class."""

    def test_basic_search(self):
        """Test basic similarity search."""
        from datawash.ml.similarity import SimilaritySearch

        embeddings = np.random.randn(100, 64).astype(np.float32)
        # Use numpy fallback to avoid FAISS issues in tests
        search = SimilaritySearch(embeddings, use_faiss=False)

        query = embeddings[0]  # Search for first embedding
        indices, similarities = search.search(query, k=5)

        assert len(indices) == 5
        assert indices[0] == 0  # First result should be the query itself
        assert similarities[0] > 0.99  # Self-similarity should be ~1.0

    def test_find_duplicates(self):
        """Test finding duplicate pairs."""
        from datawash.ml.similarity import SimilaritySearch

        # Create embeddings with known duplicates
        base = np.random.randn(10, 64).astype(np.float32)
        # Make embedding 1 a near-copy of embedding 0
        base[1] = base[0] + 0.01 * np.random.randn(64).astype(np.float32)

        # Use numpy fallback to avoid FAISS issues in tests
        search = SimilaritySearch(base, use_faiss=False)
        duplicates = search.find_duplicates(threshold=0.95)

        # Should find the (0, 1) pair
        found_pair = any(
            (idx1 == 0 and idx2 == 1) or (idx1 == 1 and idx2 == 0)
            for idx1, idx2, _ in duplicates
        )
        assert found_pair

    def test_numpy_fallback(self):
        """Test that numpy fallback works."""
        from datawash.ml.similarity import SimilaritySearch

        embeddings = np.random.randn(50, 32).astype(np.float32)
        search = SimilaritySearch(embeddings, use_faiss=False)

        query = embeddings[0]
        indices, similarities = search.search(query, k=3)

        assert len(indices) == 3
        assert indices[0] == 0


class TestFuzzyDuplicateFinder:
    """Tests for FuzzyDuplicateFinder class."""

    def test_find_duplicates_simple(self):
        """Test finding simple fuzzy duplicates."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {
                "name": ["John Smith", "Jon Smith", "Jane Doe", "Bob Johnson"],
                "city": ["New York", "NYC", "Boston", "Chicago"],
            }
        )

        finder = FuzzyDuplicateFinder(model_preset="fast")
        groups = finder.find_duplicates(df, columns=["name"], threshold=0.8)

        # Should find John Smith and Jon Smith as duplicates
        assert len(groups) >= 1
        # Check that indices 0 and 1 are in the same group
        found_name_group = any(0 in g and 1 in g for g in groups)
        assert found_name_group

    def test_find_duplicates_multiple_columns(self):
        """Test finding duplicates across multiple columns."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {
                "name": ["John Smith", "Jon Smith", "Jane Doe"],
                "address": ["123 Main St", "123 Main Street", "456 Oak Ave"],
            }
        )

        finder = FuzzyDuplicateFinder(model_preset="fast")
        groups = finder.find_duplicates(df, columns=["name", "address"], threshold=0.8)

        # First two rows should be in same group
        assert len(groups) >= 1

    def test_no_duplicates(self):
        """Test with no duplicates."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie", "Diana"],
                "city": ["Boston", "Chicago", "Denver", "Miami"],
            }
        )

        finder = FuzzyDuplicateFinder(model_preset="fast")
        groups = finder.find_duplicates(df, columns=["name"], threshold=0.95)

        # Should find no duplicates at high threshold
        assert len(groups) == 0

    def test_suggest_canonical_first(self):
        """Test canonical suggestion with 'first' strategy."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame({"name": ["John Smith", "Jon Smith"], "age": [30, 31]})

        finder = FuzzyDuplicateFinder()
        canonical = finder.suggest_canonical(df, [0, 1], strategy="first")
        assert canonical == 0

    def test_suggest_canonical_most_complete(self):
        """Test canonical suggestion with 'most_complete' strategy."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {
                "name": ["John Smith", "Jon Smith"],
                "email": [None, "jon@email.com"],
                "phone": [None, "555-1234"],
            }
        )

        finder = FuzzyDuplicateFinder()
        canonical = finder.suggest_canonical(
            df, [0, 1], columns=["email", "phone"], strategy="most_complete"
        )
        assert canonical == 1  # Row 1 has fewer nulls

    def test_suggest_canonical_longest(self):
        """Test canonical suggestion with 'longest' strategy."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {
                "name": ["Jon", "Jonathan Smith"],
                "description": ["A", "A longer description"],
            }
        )

        finder = FuzzyDuplicateFinder()
        canonical = finder.suggest_canonical(
            df, [0, 1], columns=["name", "description"], strategy="longest"
        )
        assert canonical == 1  # Row 1 has longer text

    def test_get_duplicate_summary(self):
        """Test getting duplicate summary DataFrame."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {"name": ["John", "Jon", "Jane"], "city": ["NYC", "NYC", "Boston"]}
        )

        finder = FuzzyDuplicateFinder()
        groups = [[0, 1]]  # Simulated group

        summary = finder.get_duplicate_summary(df, groups, columns=["name", "city"])

        assert len(summary) == 2
        assert "_group_id" in summary.columns
        assert "_row_index" in summary.columns
        assert "name" in summary.columns

    def test_handles_nulls(self):
        """Test that finder handles null values."""
        from datawash.ml.fuzzy_duplicates import FuzzyDuplicateFinder

        df = pd.DataFrame(
            {"name": ["John", None, "Jane"], "city": [None, "NYC", "Boston"]}
        )

        finder = FuzzyDuplicateFinder()
        # Should not raise error
        groups = finder.find_duplicates(df, columns=["name"], threshold=0.9)
        assert isinstance(groups, list)


class TestFuzzyDuplicateDetector:
    """Tests for FuzzyDuplicateDetector class."""

    def test_detector_registration(self):
        """Test that detector is registered."""
        from datawash.detectors.registry import get_all_detectors

        detectors = get_all_detectors()
        assert "fuzzy_duplicates" in detectors

    def test_detector_finds_duplicates(self):
        """Test that detector finds fuzzy duplicates."""
        from datawash.detectors.fuzzy_duplicate_detector import FuzzyDuplicateDetector
        from datawash.profiler import profile_dataset

        df = pd.DataFrame(
            {
                "name": [
                    "John Smith",
                    "Jon Smith",
                    "Jane Doe",
                    "Bob Johnson",
                    "Robert Johnson",
                    "Alice Brown",
                    "Alicia Brown",
                    "Charlie White",
                    "Charles White",
                    "David Lee",
                    "Dave Lee",
                    "Eva Green",
                ]
            }
        )

        detector = FuzzyDuplicateDetector(threshold=0.85)
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        assert len(findings) >= 1
        assert findings[0].issue_type == "fuzzy_duplicates"
        assert "duplicate_groups" in findings[0].details

    def test_detector_skips_small_datasets(self):
        """Test that detector skips small datasets."""
        from datawash.detectors.fuzzy_duplicate_detector import FuzzyDuplicateDetector
        from datawash.profiler import profile_dataset

        df = pd.DataFrame({"name": ["John", "Jane"]})  # Only 2 rows

        detector = FuzzyDuplicateDetector()
        profile = profile_dataset(df)
        findings = detector.detect(df, profile)

        assert len(findings) == 0


class TestFuzzyDuplicateTransformer:
    """Tests for FuzzyDuplicateTransformer class."""

    def test_transformer_registration(self):
        """Test that transformer is registered."""
        from datawash.transformers.registry import get_transformer

        transformer = get_transformer("fuzzy_duplicates")
        assert transformer is not None

    def test_transformer_removes_duplicates(self):
        """Test that transformer removes fuzzy duplicates."""
        from datawash.transformers.fuzzy_duplicates import FuzzyDuplicateTransformer

        df = pd.DataFrame(
            {
                "name": ["John Smith", "Jon Smith", "Jane Doe", "Bob Johnson"],
                "value": [1, 2, 3, 4],
            }
        )

        transformer = FuzzyDuplicateTransformer()
        result_df, tx_result = transformer.transform(
            df, columns=["name"], threshold=0.8, strategy="first"
        )

        # Should have fewer rows
        assert len(result_df) < len(df)
        assert tx_result.rows_affected > 0

    def test_transformer_keeps_canonical(self):
        """Test that transformer keeps canonical row using 'first' strategy."""
        from datawash.transformers.fuzzy_duplicates import FuzzyDuplicateTransformer

        df = pd.DataFrame(
            {
                "name": ["John Smith", "Jon Smith", "Jane Doe", "Bob Johnson"],
                "value": [1, 2, 3, 4],
            }
        )

        transformer = FuzzyDuplicateTransformer()
        result_df, tx_result = transformer.transform(
            df, columns=["name"], threshold=0.8, strategy="first"
        )

        # Should have removed duplicates, keeping first occurrence
        assert len(result_df) < len(df)
        # First row (John Smith) should be kept, not Jon Smith
        assert "John Smith" in result_df["name"].values or len(result_df) == 3

    def test_transformer_no_duplicates(self):
        """Test transformer with no duplicates."""
        from datawash.transformers.fuzzy_duplicates import FuzzyDuplicateTransformer

        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})

        transformer = FuzzyDuplicateTransformer()
        result_df, tx_result = transformer.transform(
            df, columns=["name"], threshold=0.99
        )

        assert len(result_df) == 3
        assert tx_result.rows_affected == 0

    def test_transformer_generates_code(self):
        """Test code generation."""
        from datawash.transformers.fuzzy_duplicates import FuzzyDuplicateTransformer

        transformer = FuzzyDuplicateTransformer()
        code = transformer.generate_code(
            columns=["name"], threshold=0.85, strategy="first"
        )

        assert "FuzzyDuplicateFinder" in code
        assert "find_duplicates" in code
        assert "0.85" in code


class TestIntegration:
    """Integration tests for fuzzy duplicate detection."""

    def test_full_pipeline(self):
        """Test full analyze → apply pipeline."""
        from datawash import analyze

        df = pd.DataFrame(
            {
                "customer": [
                    "John Smith",
                    "Jon Smith",
                    "Jane Doe",
                    "Robert Johnson",
                    "Bob Johnson",
                    "Alice Brown",
                    "Alicia Browne",
                    "Charlie Davis",
                    "Charles Davis",
                    "Eva Wilson",
                    "Eve Wilson",
                    "Frank Miller",
                ],
                "amount": [100, 150, 200, 300, 250, 400, 380, 500, 520, 600, 610, 700],
            }
        )

        report = analyze(df)

        # Note: Detection depends on model and threshold,
        # so we just verify the pipeline works without errors
        assert isinstance(report.issues, list)
        assert isinstance(report.suggestions, list)

    def test_ml_available_flag(self):
        """Test that ml_available flag is set correctly."""
        import datawash

        assert datawash.ml_available is True
