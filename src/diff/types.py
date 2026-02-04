"""
Pydantic models for diff results.

Defines structured types for all diff operations to ensure
consistent output format across the module.
"""

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field


class DiffOpcode(BaseModel):
    """Represents a single diff operation (insert, delete, replace, equal)."""

    operation: Literal["insert", "delete", "replace", "equal"] = Field(
        description="The type of diff operation"
    )
    a_start: int = Field(description="Start index in sequence A")
    a_end: int = Field(description="End index in sequence A")
    b_start: int = Field(description="Start index in sequence B")
    b_end: int = Field(description="End index in sequence B")
    a_content: Optional[str] = Field(default=None, description="Content from sequence A")
    b_content: Optional[str] = Field(default=None, description="Content from sequence B")


class MatchingBlock(BaseModel):
    """Represents a matching block between two sequences."""

    a_start: int = Field(description="Start index in sequence A")
    b_start: int = Field(description="Start index in sequence B")
    size: int = Field(description="Length of the matching block")
    content: Optional[str] = Field(default=None, description="The matching content")


class TextDiffResult(BaseModel):
    """Result of comparing two text strings."""

    text_a: str = Field(description="First text string")
    text_b: str = Field(description="Second text string")
    similarity: float = Field(ge=0.0, le=1.0, description="Similarity ratio (0.0 to 1.0)")
    similarity_percent: str = Field(description="Similarity as percentage string")
    is_identical: bool = Field(description="Whether the texts are identical")
    opcodes: List[DiffOpcode] = Field(default_factory=list, description="List of diff operations")
    matching_blocks: List[MatchingBlock] = Field(default_factory=list, description="List of matching blocks")
    unified_diff: Optional[str] = Field(default=None, description="Unified diff format")
    ndiff: Optional[str] = Field(default=None, description="Character-level diff with +/-/?")
    explanation: Optional[str] = Field(default=None, description="Human-readable explanation")


class ListDiffResult(BaseModel):
    """Result of comparing two lists."""

    list_a_count: int = Field(description="Count of items in list A")
    list_b_count: int = Field(description="Count of items in list B")
    added: List[Any] = Field(default_factory=list, description="Items in B but not in A")
    removed: List[Any] = Field(default_factory=list, description="Items in A but not in B")
    common: List[Any] = Field(default_factory=list, description="Items in both lists")
    added_count: int = Field(description="Count of added items")
    removed_count: int = Field(description="Count of removed items")
    common_count: int = Field(description="Count of common items")
    jaccard_similarity: float = Field(ge=0.0, le=1.0, description="Jaccard similarity index")
    jaccard_percent: str = Field(description="Jaccard similarity as percentage")
    sequence_similarity: float = Field(ge=0.0, le=1.0, description="Sequence-based similarity")


class DictValueDiff(BaseModel):
    """Diff result for a single dictionary value."""

    key: str = Field(description="The dictionary key")
    value_a: Any = Field(description="Value in dict A")
    value_b: Any = Field(description="Value in dict B")
    status: Literal["added", "removed", "changed", "unchanged"] = Field(
        description="Type of change"
    )
    similarity: Optional[float] = Field(
        default=None,
        description="String similarity if both values are strings"
    )
    opcodes: Optional[List[DiffOpcode]] = Field(
        default=None,
        description="Character-level diff if both values are strings"
    )


class DictDiffResult(BaseModel):
    """Result of comparing two dictionaries."""

    dict_a_keys: int = Field(description="Count of keys in dict A")
    dict_b_keys: int = Field(description="Count of keys in dict B")
    added_keys: List[str] = Field(default_factory=list, description="Keys in B but not in A")
    removed_keys: List[str] = Field(default_factory=list, description="Keys in A but not in B")
    common_keys: List[str] = Field(default_factory=list, description="Keys in both dicts")
    changed_keys: List[str] = Field(default_factory=list, description="Common keys with different values")
    unchanged_keys: List[str] = Field(default_factory=list, description="Common keys with same values")
    differences: List[DictValueDiff] = Field(
        default_factory=list,
        description="Detailed diff for each key"
    )
    overall_similarity: float = Field(
        ge=0.0, le=1.0,
        description="Overall similarity score"
    )


class SimilarStringMatch(BaseModel):
    """A similar string match result."""

    candidate: str = Field(description="The matching candidate string")
    similarity: float = Field(ge=0.0, le=1.0, description="Similarity score")
    similarity_percent: str = Field(description="Similarity as percentage")
    rank: int = Field(description="Rank among matches (1 = best)")


class PatchResult(BaseModel):
    """Result of generating a patch between two texts."""

    format: Literal["unified", "context", "ndiff"] = Field(description="Patch format")
    patch: str = Field(description="The patch content")
    from_label: str = Field(description="Label for the source")
    to_label: str = Field(description="Label for the target")
    line_count: int = Field(description="Number of lines in the patch")


class TransformDiff(BaseModel):
    """Diff result for a single value transformation."""

    index: int = Field(description="Row index")
    before: str = Field(description="Value before transformation")
    after: str = Field(description="Value after transformation")
    similarity: float = Field(ge=0.0, le=1.0, description="Similarity between before/after")
    opcodes: List[DiffOpcode] = Field(default_factory=list, description="Character-level changes")
    explanation: str = Field(description="Human-readable change description")
