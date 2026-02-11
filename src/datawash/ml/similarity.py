"""Similarity search utilities using FAISS or numpy fallback."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from numpy.typing import NDArray


def _has_faiss() -> bool:
    """Check if FAISS is available."""
    try:
        import faiss  # noqa: F401

        return True
    except ImportError:
        return False


FAISS_AVAILABLE = _has_faiss()


class SimilaritySearch:
    """Efficient similarity search for embeddings.

    Uses FAISS for fast approximate nearest neighbor search when available,
    falls back to numpy brute-force search otherwise.

    Example:
        >>> embeddings = np.random.randn(1000, 384).astype(np.float32)
        >>> search = SimilaritySearch(embeddings)
        >>> query = np.random.randn(384).astype(np.float32)
        >>> indices, distances = search.search(query, k=5)
    """

    def __init__(
        self,
        embeddings: NDArray[np.float32],
        use_faiss: bool = False,
        normalize: bool = True,
    ):
        """Initialize the similarity search index.

        Args:
            embeddings: Array of shape (n_samples, embedding_dim).
            use_faiss: Use FAISS if available. Default False (numpy is more stable).
            normalize: L2 normalize embeddings for cosine similarity. Default True.
        """
        self._embeddings = np.ascontiguousarray(embeddings, dtype=np.float32)

        if normalize:
            norms = np.linalg.norm(self._embeddings, axis=1, keepdims=True)
            norms = np.maximum(norms, 1e-10)  # Avoid division by zero
            self._embeddings = self._embeddings / norms

        self._use_faiss = use_faiss and FAISS_AVAILABLE
        self._index = None

        if self._use_faiss:
            self._build_faiss_index()

    def _build_faiss_index(self) -> None:
        """Build FAISS index for fast search."""
        import faiss

        dim = self._embeddings.shape[1]
        # Use inner product (equivalent to cosine for normalized vectors)
        self._index = faiss.IndexFlatIP(dim)
        self._index.add(self._embeddings)

    @property
    def n_samples(self) -> int:
        """Number of samples in the index."""
        return len(self._embeddings)

    @property
    def embedding_dim(self) -> int:
        """Dimension of embeddings."""
        return self._embeddings.shape[1]

    def search(
        self,
        query: NDArray[np.float32],
        k: int = 5,
    ) -> tuple[NDArray[np.int64], NDArray[np.float32]]:
        """Find k nearest neighbors for a query embedding.

        Args:
            query: Query embedding of shape (embedding_dim,) or (n_queries, embedding_dim).
            k: Number of nearest neighbors to return.

        Returns:
            Tuple of (indices, similarities) arrays.
            - indices: Shape (k,) or (n_queries, k) with indices of nearest neighbors.
            - similarities: Shape (k,) or (n_queries, k) with similarity scores.
        """
        query = np.ascontiguousarray(query, dtype=np.float32)
        single_query = query.ndim == 1

        if single_query:
            query = query.reshape(1, -1)

        # Normalize query
        norms = np.linalg.norm(query, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-10)
        query = query / norms

        k = min(k, self.n_samples)

        if self._use_faiss and self._index is not None:
            similarities, indices = self._index.search(query, k)
        else:
            # Numpy fallback: brute-force search
            similarities_matrix = np.dot(query, self._embeddings.T)
            indices = np.argsort(-similarities_matrix, axis=1)[:, :k]
            similarities = np.take_along_axis(similarities_matrix, indices, axis=1)

        indices = indices.astype(np.int64)
        similarities = similarities.astype(np.float32)

        if single_query:
            return indices[0], similarities[0]
        return indices, similarities

    def find_duplicates(
        self,
        threshold: float = 0.9,
        batch_size: int = 1000,
    ) -> list[tuple[int, int, float]]:
        """Find all pairs of similar embeddings above threshold.

        Args:
            threshold: Minimum similarity threshold (0-1). Default 0.9.
            batch_size: Batch size for processing. Default 1000.

        Returns:
            List of (idx1, idx2, similarity) tuples for similar pairs.
            Only returns pairs where idx1 < idx2.
        """
        duplicates: list[tuple[int, int, float]] = []
        n = self.n_samples

        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            batch = self._embeddings[start:end]

            # Search for neighbors with similarity above threshold
            # Add 1 to k to account for self-match
            k = min(100, n)  # Limit to avoid memory issues
            indices, similarities = self.search(batch, k=k)

            for i, (idx_row, sim_row) in enumerate(zip(indices, similarities)):
                query_idx = start + i
                for neighbor_idx, sim in zip(idx_row, sim_row):
                    # Skip self-matches and pairs where query_idx >= neighbor_idx
                    if neighbor_idx <= query_idx:
                        continue
                    if sim >= threshold:
                        duplicates.append((query_idx, int(neighbor_idx), float(sim)))

        return duplicates

    def find_similar(
        self,
        query: NDArray[np.float32],
        threshold: float = 0.8,
        max_results: int = 100,
    ) -> list[tuple[int, float]]:
        """Find all embeddings similar to a query above threshold.

        Args:
            query: Query embedding of shape (embedding_dim,).
            threshold: Minimum similarity threshold. Default 0.8.
            max_results: Maximum number of results. Default 100.

        Returns:
            List of (index, similarity) tuples, sorted by similarity descending.
        """
        k = min(max_results, self.n_samples)
        indices, similarities = self.search(query, k=k)

        results = [
            (int(idx), float(sim))
            for idx, sim in zip(indices, similarities)
            if sim >= threshold
        ]

        return results
