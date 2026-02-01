"""
Pydantic models for CASE statement extraction and hierarchy detection.

These models represent extracted CASE WHEN statements and the
hierarchies that can be inferred from their logic.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ConditionOperator(str, Enum):
    """Operators used in CASE conditions."""
    EQUALS = "="
    NOT_EQUALS = "<>"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="
    LESS_THAN = "<"
    LESS_EQUAL = "<="
    REGEXP = "REGEXP"
    AND = "AND"
    OR = "OR"


class EntityType(str, Enum):
    """
    Standard entity types that can be detected from CASE statements.

    The discovery engine detects these 12 entity types for hierarchy building.
    """
    ACCOUNT = "account"
    COST_CENTER = "cost_center"
    DEPARTMENT = "department"
    ENTITY = "entity"
    PROJECT = "project"
    PRODUCT = "product"
    CUSTOMER = "customer"
    VENDOR = "vendor"
    EMPLOYEE = "employee"
    LOCATION = "location"
    TIME_PERIOD = "time_period"
    CURRENCY = "currency"
    UNKNOWN = "unknown"


class CaseCondition(BaseModel):
    """
    Represents a single condition within a CASE WHEN clause.

    For example, in "WHEN account_code ILIKE '501%' THEN 'Oil Sales'"
    - column: "account_code"
    - operator: ILIKE
    - values: ["501%"]
    """

    column: str = Field(..., description="Column being tested")
    operator: ConditionOperator = Field(..., description="Comparison operator")
    values: list[str] = Field(default_factory=list, description="Values being compared")
    is_negated: bool = Field(default=False, description="True if condition is negated")
    raw_condition: str = Field(..., description="Original SQL condition text")

    # For compound conditions (AND/OR)
    left_condition: CaseCondition | None = Field(None, description="Left side of compound condition")
    right_condition: CaseCondition | None = Field(None, description="Right side of compound condition")

    model_config = {"extra": "allow"}

    @property
    def is_compound(self) -> bool:
        """True if this is a compound (AND/OR) condition."""
        return self.operator in (ConditionOperator.AND, ConditionOperator.OR)

    @property
    def is_pattern_match(self) -> bool:
        """True if this condition uses pattern matching (LIKE/ILIKE)."""
        return self.operator in (ConditionOperator.LIKE, ConditionOperator.ILIKE, ConditionOperator.REGEXP)

    def get_pattern_prefix(self) -> str | None:
        """Extract prefix from LIKE pattern (e.g., '501%' -> '501')."""
        if not self.is_pattern_match or not self.values:
            return None
        value = self.values[0]
        # Remove wildcards to get prefix
        return value.rstrip('%').rstrip('_')


class CaseWhen(BaseModel):
    """
    Represents a single WHEN...THEN branch in a CASE statement.

    Includes the condition(s) that trigger this branch and the result value.
    """

    condition: CaseCondition = Field(..., description="The WHEN condition")
    result_value: str = Field(..., description="The THEN result value")
    result_type: str = Field(default="string", description="Inferred type of result")
    position: int = Field(default=0, description="Position in CASE statement")
    raw_when_clause: str = Field(..., description="Original WHEN...THEN SQL")

    # Hierarchy inference
    inferred_level: int | None = Field(None, description="Inferred hierarchy level")
    inferred_parent: str | None = Field(None, description="Inferred parent in hierarchy")

    model_config = {"extra": "allow"}


class CaseStatement(BaseModel):
    """
    Complete representation of a CASE statement extracted from SQL.

    This model captures all WHEN branches plus the ELSE clause,
    along with metadata about the source and detected patterns.
    """

    id: str = Field(..., description="Unique identifier for this CASE statement")
    source_column: str = Field(..., description="Column name this CASE creates")
    input_column: str = Field(..., description="Primary input column being tested")
    input_table: str | None = Field(None, description="Table of input column")

    # CASE branches
    when_clauses: list[CaseWhen] = Field(default_factory=list, description="WHEN...THEN branches")
    else_value: str | None = Field(None, description="ELSE value if present")

    # Detection metadata
    detected_entity_type: EntityType = Field(default=EntityType.UNKNOWN, description="Detected entity type")
    detected_pattern: str | None = Field(None, description="Pattern type: prefix, suffix, exact, etc.")
    unique_result_values: list[str] = Field(default_factory=list, description="All unique THEN values")
    condition_count: int = Field(default=0, description="Total number of conditions")

    # Raw SQL
    raw_case_sql: str = Field(..., description="Original CASE statement SQL")
    position_in_query: int = Field(default=0, description="Position in SELECT clause")

    model_config = {"extra": "allow"}

    def get_result_values(self) -> list[str]:
        """Get all unique result values from WHEN clauses."""
        values = [w.result_value for w in self.when_clauses]
        if self.else_value:
            values.append(self.else_value)
        return list(set(values))

    def get_input_values(self) -> list[str]:
        """Get all input values from conditions."""
        values = []
        for when in self.when_clauses:
            values.extend(when.condition.values)
        return values

    def count_by_result(self) -> dict[str, int]:
        """Count how many conditions map to each result value."""
        counts: dict[str, int] = {}
        for when in self.when_clauses:
            result = when.result_value
            counts[result] = counts.get(result, 0) + 1
        return counts


class HierarchyLevel(BaseModel):
    """
    Represents a single level in an extracted hierarchy.

    Maps CASE result values to hierarchy positions.
    """

    level_number: int = Field(..., description="Level number (1 = top)")
    level_name: str = Field(..., description="Name of this level (e.g., 'Category')")
    values: list[str] = Field(default_factory=list, description="Values at this level")
    parent_level: int | None = Field(None, description="Parent level number")
    sort_order_map: dict[str, int] = Field(default_factory=dict, description="Value -> sort order mapping")

    model_config = {"extra": "allow"}


class ExtractedHierarchy(BaseModel):
    """
    A hierarchy extracted from CASE statement analysis.

    This represents the hierarchical structure inferred from
    CASE WHEN logic, ready for Librarian project generation.
    """

    id: str = Field(..., description="Unique identifier")
    name: str = Field(..., description="Hierarchy name")
    source_case_id: str = Field(..., description="Source CASE statement ID")
    entity_type: EntityType = Field(default=EntityType.UNKNOWN, description="Detected entity type")

    # Structure
    levels: list[HierarchyLevel] = Field(default_factory=list, description="Hierarchy levels")
    total_levels: int = Field(default=0, description="Total number of levels")
    total_nodes: int = Field(default=0, description="Total number of hierarchy nodes")

    # Mappings
    value_to_node: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        description="Input value -> hierarchy node mapping"
    )

    # Source metadata
    source_column: str = Field(..., description="Column this hierarchy is based on")
    source_table: str | None = Field(None, description="Source table")

    # Confidence
    confidence_score: float = Field(default=0.0, description="Confidence in extraction (0-1)")
    confidence_notes: list[str] = Field(default_factory=list, description="Notes about confidence")

    model_config = {"extra": "allow"}

    def get_level_values(self, level_number: int) -> list[str]:
        """Get all values at a specific level."""
        for level in self.levels:
            if level.level_number == level_number:
                return level.values
        return []

    def to_librarian_hierarchy_rows(self) -> list[dict[str, Any]]:
        """
        Convert to Librarian HIERARCHY.CSV format rows.

        Returns list of dicts with Librarian hierarchy columns.
        """
        rows = []
        for level in self.levels:
            for idx, value in enumerate(level.values):
                row = {
                    "HIERARCHY_ID": f"{self.id}_{level.level_number}_{idx}",
                    "HIERARCHY_NAME": value,
                    "PARENT_ID": None,  # Needs to be computed based on relationships
                    f"LEVEL_{level.level_number}": value,
                    f"LEVEL_{level.level_number}_SORT": level.sort_order_map.get(value, idx),
                    "INCLUDE_FLAG": True,
                    "FORMULA_GROUP": None,
                }
                rows.append(row)
        return rows
