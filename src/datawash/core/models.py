"""Data models for datawash."""

from __future__ import annotations

import enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class Severity(str, enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ColumnProfile(BaseModel):
    """Profile for a single column."""

    name: str
    dtype: str
    semantic_type: Optional[str] = None
    null_count: int = 0
    null_ratio: float = 0.0
    unique_count: int = 0
    unique_ratio: float = 0.0
    sample_values: list[Any] = Field(default_factory=list)
    statistics: dict[str, Any] = Field(default_factory=dict)
    patterns: dict[str, Any] = Field(default_factory=dict)


class DatasetProfile(BaseModel):
    """Profile for an entire dataset."""

    row_count: int
    column_count: int
    memory_bytes: int
    columns: dict[str, ColumnProfile] = Field(default_factory=dict)
    duplicate_row_count: int = 0


class Finding(BaseModel):
    """A detected data quality issue."""

    detector: str
    issue_type: str
    severity: Severity
    columns: list[str] = Field(default_factory=list)
    rows: Optional[list[int]] = None
    details: dict[str, Any] = Field(default_factory=dict)
    message: str
    confidence: float = 1.0


class Suggestion(BaseModel):
    """A suggested fix for a finding."""

    id: int
    finding: Finding
    action: str
    transformer: str
    params: dict[str, Any] = Field(default_factory=dict)
    priority: Severity
    impact: str
    rationale: str
    preview: Optional[str] = None


class TransformationResult(BaseModel):
    """Result of applying a transformation."""

    transformer: str
    params: dict[str, Any] = Field(default_factory=dict)
    rows_affected: int
    columns_affected: list[str] = Field(default_factory=list)
    code: str = ""
