"""Configuration management for datawash."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class DetectorConfig(BaseModel):
    enabled: list[str] = Field(
        default_factory=lambda: [
            "missing",
            "duplicates",
            "types",
            "formats",
            "outliers",
            "similarity",
        ]
    )
    similarity_name_threshold: float = 0.8
    similarity_value_threshold: float = 0.7
    outlier_method: Literal["iqr", "zscore"] = "iqr"
    outlier_threshold: float = 1.5
    fuzzy_duplicates_enabled: bool = False
    fuzzy_duplicates_threshold: float = 0.85


class MLConfig(BaseModel):
    embedding_model: str = "all-MiniLM-L6-v2"
    device: str = "cpu"


class SuggestionConfig(BaseModel):
    max_suggestions: int = 50
    min_confidence: float = 0.7


class CodegenConfig(BaseModel):
    style: Literal["function", "script"] = "function"
    include_comments: bool = True


class Config(BaseModel):
    sample_size: int = 10000
    max_unique_ratio: float = 0.95
    null_threshold: float = 0.5
    detectors: DetectorConfig = Field(default_factory=DetectorConfig)
    ml: MLConfig = Field(default_factory=MLConfig)
    suggestions: SuggestionConfig = Field(default_factory=SuggestionConfig)
    codegen: CodegenConfig = Field(default_factory=CodegenConfig)
    use_case: Literal["general", "ml", "analytics", "export"] = "general"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Config:
        return cls.model_validate(data)
