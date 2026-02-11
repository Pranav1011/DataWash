"""Text embedding utilities using sentence-transformers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


class TextEmbedder:
    """Generate text embeddings using sentence-transformers.

    Uses the lightweight 'all-MiniLM-L6-v2' model by default,
    which provides good quality embeddings with fast inference.

    Example:
        >>> embedder = TextEmbedder()
        >>> embeddings = embedder.embed(["hello world", "hi there"])
        >>> print(embeddings.shape)  # (2, 384)

        >>> similarity = embedder.similarity("John Smith", "Jon Smith")
        >>> print(f"Similarity: {similarity:.2f}")  # ~0.92
    """

    # Default model - good balance of speed and quality
    DEFAULT_MODEL = "all-MiniLM-L6-v2"

    # Alternative models for different needs
    MODELS = {
        "fast": "all-MiniLM-L6-v2",  # 384 dims, fastest
        "balanced": "all-mpnet-base-v2",  # 768 dims, better quality
        "multilingual": "paraphrase-multilingual-MiniLM-L12-v2",  # 384 dims, 50+ langs
    }

    def __init__(
        self,
        model_name: str | None = None,
        model_preset: str = "fast",
        device: str | None = None,
        cache_folder: str | None = None,
    ):
        """Initialize the text embedder.

        Args:
            model_name: Specific model name from HuggingFace. If None, uses preset.
            model_preset: One of "fast", "balanced", "multilingual". Default "fast".
            device: Device to use ("cpu", "cuda", "mps"). Auto-detected if None.
            cache_folder: Folder to cache downloaded models.
        """
        from sentence_transformers import SentenceTransformer

        if model_name is None:
            model_name = self.MODELS.get(model_preset, self.DEFAULT_MODEL)

        self.model_name = model_name
        self.model: SentenceTransformer = SentenceTransformer(
            model_name,
            device=device,
            cache_folder=cache_folder,
        )
        self._embedding_dim = self.model.get_sentence_embedding_dimension()

    @property
    def embedding_dim(self) -> int:
        """Dimension of the embedding vectors."""
        return self._embedding_dim

    def embed(
        self,
        texts: list[str],
        batch_size: int = 32,
        show_progress: bool = False,
        normalize: bool = True,
    ) -> np.ndarray:
        """Generate embeddings for a list of texts.

        Args:
            texts: List of strings to embed.
            batch_size: Batch size for encoding. Default 32.
            show_progress: Show progress bar. Default False.
            normalize: L2 normalize embeddings. Default True (for cosine similarity).

        Returns:
            numpy array of shape (len(texts), embedding_dim)
        """
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress,
            normalize_embeddings=normalize,
            convert_to_numpy=True,
        )
        return embeddings  # type: ignore[return-value]

    def embed_single(self, text: str, normalize: bool = True) -> np.ndarray:
        """Generate embedding for a single text.

        Args:
            text: String to embed.
            normalize: L2 normalize embedding. Default True.

        Returns:
            numpy array of shape (embedding_dim,)
        """
        return self.embed([text], normalize=normalize)[0]

    def similarity(self, text1: str, text2: str) -> float:
        """Compute cosine similarity between two texts.

        Args:
            text1: First text.
            text2: Second text.

        Returns:
            Cosine similarity score between 0 and 1.
        """
        embeddings = self.embed([text1, text2], normalize=True)
        # For normalized vectors, dot product = cosine similarity
        return float(np.dot(embeddings[0], embeddings[1]))

    def pairwise_similarity(self, texts: list[str]) -> np.ndarray:
        """Compute pairwise similarity matrix for a list of texts.

        Args:
            texts: List of strings.

        Returns:
            numpy array of shape (len(texts), len(texts)) with similarity scores.
        """
        embeddings = self.embed(texts, normalize=True)
        # For normalized vectors, similarity matrix = embeddings @ embeddings.T
        return np.dot(embeddings, embeddings.T)
