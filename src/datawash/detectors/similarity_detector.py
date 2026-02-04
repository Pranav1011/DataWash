"""Similar column detector using MinHash + LSH.

Multi-stage similarity detection with O(n) average complexity:
1. N-gram blocking for column names
2. MinHash + LSH for column values
3. Size filtering to prune candidates
4. Exact verification only on candidates

Based on:
- VLDB 2016: "An Empirical Evaluation of Set Similarity Join Techniques"
- Broder 1997: "On the resemblance and containment of documents" (MinHash)
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from datawash.core.models import DatasetProfile, Finding, Severity
from datawash.detectors.base import BaseDetector
from datawash.detectors.registry import register_detector


class SimilarityDetector(BaseDetector):
    """Detect similar columns using MinHash + LSH.

    Uses a multi-stage pipeline:
    - Stage 1: N-gram blocking for name similarity
    - Stage 2: MinHash signatures + LSH banding for value similarity
    - Stage 3: Exact verification only on candidate pairs
    """

    # Thresholds
    NAME_SIMILARITY_THRESHOLD = 0.7
    VALUE_OVERLAP_THRESHOLD = 0.8
    COMBINED_THRESHOLD = 0.6

    # Algorithm parameters
    NGRAM_SIZE = 2
    MIN_SHARED_NGRAMS = 2
    MINHASH_SIGNATURES = 100
    LSH_BANDS = 20
    MAX_UNIQUE_VALUES = 10000
    MAX_CANDIDATES = 5000  # Cap to avoid O(n^2) blowup on wide datasets

    def __init__(
        self, name_threshold: float = 0.7, value_threshold: float = 0.8
    ) -> None:
        self._name_threshold = name_threshold
        self._value_threshold = value_threshold

    @property
    def name(self) -> str:
        return "similarity"

    @property
    def description(self) -> str:
        return "Detects similar or potentially duplicate columns"

    def detect(
        self, df: pd.DataFrame, profile: DatasetProfile, config: Any = None
    ) -> list[Finding]:
        columns = list(df.columns)
        n_cols = len(columns)

        if n_cols < 2:
            return []

        # Precompute value sets (use unique() first to reduce work)
        column_value_sets: dict[int, set[str]] = {}
        for idx, col in enumerate(columns):
            unique_vals = df[col].dropna().unique()
            if len(unique_vals) > self.MAX_UNIQUE_VALUES:
                column_value_sets[idx] = set()  # Too many unique values (likely IDs)
            else:
                column_value_sets[idx] = set(str(v) for v in unique_vals)

        # Stage 1: Blocking
        name_candidates = self._ngram_blocking(columns)
        value_candidates = self._minhash_lsh_blocking(columns, column_value_sets)

        # Stage 2: Merge candidates (cap to avoid blowup)
        all_candidates = name_candidates | value_candidates
        if len(all_candidates) > self.MAX_CANDIDATES:
            all_candidates = set(list(all_candidates)[: self.MAX_CANDIDATES])

        # Stage 3: Verify only candidates
        findings: list[Finding] = []
        for i, j in all_candidates:
            finding = self._verify_pair(columns, i, j, column_value_sets)
            if finding:
                findings.append(finding)

        return findings

    # -----------------------------------------------------------------
    # STAGE 1a: N-GRAM BLOCKING FOR NAMES
    # -----------------------------------------------------------------

    def _ngram_blocking(self, columns: list[str]) -> set[tuple[int, int]]:
        """Find candidate pairs based on shared character n-grams.

        Complexity: O(n * k) where k = average column name length
        """
        ngram_index: dict[str, list[int]] = defaultdict(list)

        for idx, col in enumerate(columns):
            ngrams = self._get_ngrams(col.lower())
            for ng in ngrams:
                ngram_index[ng].append(idx)

        pair_counts: Counter[tuple[int, int]] = Counter()
        for indices in ngram_index.values():
            if len(indices) < 2:
                continue
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    pair = (min(indices[i], indices[j]), max(indices[i], indices[j]))
                    pair_counts[pair] += 1

        return {
            pair
            for pair, count in pair_counts.items()
            if count >= self.MIN_SHARED_NGRAMS
        }

    def _get_ngrams(self, s: str) -> set[str]:
        """Extract character n-grams from string."""
        n = self.NGRAM_SIZE
        if len(s) < n:
            return {s}
        return {s[i : i + n] for i in range(len(s) - n + 1)}

    # -----------------------------------------------------------------
    # STAGE 1b + 2: MINHASH + LSH BLOCKING FOR VALUES
    # -----------------------------------------------------------------

    def _minhash_lsh_blocking(
        self,
        columns: list[str],
        column_value_sets: dict[int, set[str]],
    ) -> set[tuple[int, int]]:
        """Find candidate pairs using MinHash signatures + LSH banding.

        MinHash: O(n * v * m) where m = signature size
        LSH:     O(n * b) where b = number of bands
        """
        n_cols = len(columns)

        # Generate MinHash signatures for each column
        signatures: list[list[int]] = []
        sizes: list[int] = []
        for idx in range(n_cols):
            val_set = column_value_sets.get(idx, set())
            signatures.append(self._minhash_signature(val_set))
            sizes.append(len(val_set))

        # LSH Banding: hash signature bands into buckets
        candidates: set[tuple[int, int]] = set()
        rows_per_band = self.MINHASH_SIGNATURES // self.LSH_BANDS

        # Size filter ratio: for Jaccard >= t, ratio must be <= (2-t)/t
        max_ratio = (2 - self._value_threshold) / self._value_threshold

        for band_idx in range(self.LSH_BANDS):
            buckets: dict[int, list[int]] = defaultdict(list)
            start = band_idx * rows_per_band
            end = start + rows_per_band

            for col_idx, sig in enumerate(signatures):
                band_hash = hash(tuple(sig[start:end]))
                buckets[band_hash].append(col_idx)

            for bucket_cols in buckets.values():
                if len(bucket_cols) < 2 or len(bucket_cols) > 50:
                    # Skip very large buckets (too many false positives)
                    continue
                for i in range(len(bucket_cols)):
                    for j in range(i + 1, len(bucket_cols)):
                        idx1, idx2 = bucket_cols[i], bucket_cols[j]
                        s1, s2 = sizes[idx1], sizes[idx2]
                        if s1 == 0 or s2 == 0:
                            continue
                        ratio = max(s1, s2) / min(s1, s2)
                        if ratio <= max_ratio:
                            candidates.add((min(idx1, idx2), max(idx1, idx2)))
                    if len(candidates) > self.MAX_CANDIDATES:
                        break
                if len(candidates) > self.MAX_CANDIDATES:
                    break

        return candidates

    def _minhash_signature(self, values: set[str]) -> list[int]:
        """Generate MinHash signature for a set of values."""
        if not values:
            return [0] * self.MINHASH_SIGNATURES
        signature = []
        for seed in range(self.MINHASH_SIGNATURES):
            min_hash = min(hash((seed, v)) % (2**32) for v in values)
            signature.append(min_hash)
        return signature

    # -----------------------------------------------------------------
    # STAGE 3: VERIFICATION
    # -----------------------------------------------------------------

    def _verify_pair(
        self,
        columns: list[str],
        i: int,
        j: int,
        column_value_sets: dict[int, set[str]],
    ) -> Finding | None:
        """Verify if a candidate pair is actually similar."""
        col1, col2 = columns[i], columns[j]

        name_sim = self._normalized_levenshtein(col1.lower(), col2.lower())
        set1 = column_value_sets.get(i, set())
        set2 = column_value_sets.get(j, set())
        value_sim = self._jaccard_similarity(set1, set2)

        combined_score = 0.4 * name_sim + 0.6 * value_sim
        if combined_score < self.COMBINED_THRESHOLD:
            return None

        severity = Severity.MEDIUM if combined_score > 0.8 else Severity.LOW

        return Finding(
            detector=self.name,
            issue_type="similar_columns",
            severity=severity,
            columns=[col1, col2],
            details={
                "name_similarity": round(name_sim, 3),
                "value_similarity": round(value_sim, 3),
                "combined_score": round(combined_score, 3),
            },
            message=(
                f"Columns '{col1}' and '{col2}' appear similar "
                f"(name: {name_sim:.0%}, values: {value_sim:.0%})"
            ),
            confidence=combined_score,
        )

    # -----------------------------------------------------------------
    # HELPERS
    # -----------------------------------------------------------------

    def _normalized_levenshtein(self, s1: str, s2: str) -> float:
        """Calculate normalized Levenshtein similarity (0 to 1)."""
        if s1 == s2:
            return 1.0
        len1, len2 = len(s1), len(s2)
        max_len = max(len1, len2)
        if max_len == 0:
            return 1.0

        # Early termination if length difference too large
        if abs(len1 - len2) / max_len > (1 - self._name_threshold):
            return 0.0

        # Two-row Levenshtein
        if len1 > len2:
            s1, s2 = s2, s1
            len1, len2 = len2, len1

        previous_row = list(range(len2 + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        distance = previous_row[-1]
        return 1 - (distance / max_len)

    def _jaccard_similarity(self, set1: set[str], set2: set[str]) -> float:
        """Calculate exact Jaccard similarity."""
        if not set1 and not set2:
            return 1.0
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0


register_detector(SimilarityDetector())
