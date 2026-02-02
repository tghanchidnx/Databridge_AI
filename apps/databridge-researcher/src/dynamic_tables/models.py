"""
Models for Dynamic Tables module.

Defines data structures for:
- DT_2: Dynamic tables built from VW_1 views
- DT_3A: Intermediate aggregations with precedence
- DT_3: Output tables with formula calculations
"""

from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
import uuid

# Try to import shared enums from databridge-models
try:
    from databridge_models.enums import (
        TableStatus,
        JoinType,
        AggregationType,
        FormulaType,
        SQLDialect,
    )
    _HAS_SHARED_ENUMS = True
except ImportError:
    _HAS_SHARED_ENUMS = False

    # Fallback: define enums locally if shared library not available
    class TableStatus(str, Enum):
        """Status of a dynamic table."""
        DRAFT = "draft"
        VALIDATED = "validated"
        DEPLOYED = "deployed"
        DEPRECATED = "deprecated"

    class JoinType(str, Enum):
        """Types of SQL joins."""
        INNER = "INNER"
        LEFT = "LEFT"
        RIGHT = "RIGHT"
        FULL = "FULL"
        CROSS = "CROSS"

    class AggregationType(str, Enum):
        """Types of aggregations."""
        SUM = "SUM"
        AVG = "AVG"
        COUNT = "COUNT"
        COUNT_DISTINCT = "COUNT_DISTINCT"
        MIN = "MIN"
        MAX = "MAX"
        FIRST = "FIRST"
        LAST = "LAST"

    class FormulaType(str, Enum):
        """Types of formula operations."""
        ADD = "ADD"
        SUBTRACT = "SUBTRACT"
        MULTIPLY = "MULTIPLY"
        DIVIDE = "DIVIDE"
        PERCENT = "PERCENT"
        VARIANCE = "VARIANCE"
        EXPRESSION = "EXPRESSION"

    class SQLDialect(str, Enum):
        """Supported SQL dialects."""
        SNOWFLAKE = "snowflake"
        POSTGRESQL = "postgresql"
        TSQL = "tsql"
        MYSQL = "mysql"


# =============================================================================
# PYDANTIC MODELS (Stateless/API)
# =============================================================================


class DynamicTableColumn(BaseModel):
    """Column definition for dynamic tables."""
    name: str
    source_column: Optional[str] = None
    alias: Optional[str] = None
    data_type: Optional[str] = None
    expression: Optional[str] = None
    aggregation: Optional[AggregationType] = None
    is_dimension: bool = False
    is_measure: bool = False


class JoinDefinition(BaseModel):
    """Join definition for dynamic tables."""
    table: str
    alias: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    join_type: JoinType = JoinType.LEFT
    on_condition: str  # e.g., "f.account_id = d.id"


class FilterDefinition(BaseModel):
    """Filter definition with precedence support."""
    column: str
    operator: str = "="
    value: Any = None
    values: Optional[List[Any]] = None  # For IN clause
    expression: Optional[str] = None  # For complex expressions
    precedence_group: int = 1  # Lower = higher priority


class AggregationDefinition(BaseModel):
    """Aggregation definition for measures."""
    column: str
    function: AggregationType = AggregationType.SUM
    alias: Optional[str] = None
    distinct: bool = False
    filter_condition: Optional[str] = None  # For FILTER (WHERE ...) clause


class FormulaColumn(BaseModel):
    """Formula column definition for output tables."""
    alias: str
    formula_type: FormulaType
    operands: List[str]  # Column names or values
    expression: Optional[str] = None  # For EXPRESSION type
    round_decimals: Optional[int] = None


# =============================================================================
# DYNAMIC TABLE (DT_2)
# =============================================================================


class DynamicTable(BaseModel):
    """
    DT_2: Dynamic table built from VW_1 views.

    Represents a dynamic table with joins, filters, groupings, and aggregations.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_id: str
    table_name: str
    display_name: Optional[str] = None
    description: Optional[str] = None

    # Source (VW_1)
    source_view_id: Optional[str] = None
    source_view_name: Optional[str] = None
    source_database: Optional[str] = None
    source_schema: Optional[str] = None

    # Target
    target_database: Optional[str] = None
    target_schema: Optional[str] = None
    target_lag: str = "1 hour"  # Snowflake dynamic table lag
    warehouse: str = "COMPUTE_WH"

    # Structure
    columns: List[DynamicTableColumn] = Field(default_factory=list)
    joins: List[JoinDefinition] = Field(default_factory=list)
    filters: List[FilterDefinition] = Field(default_factory=list)
    group_by: List[str] = Field(default_factory=list)
    aggregations: List[AggregationDefinition] = Field(default_factory=list)

    # Generated SQL
    generated_sql: Optional[str] = None
    dialect: SQLDialect = SQLDialect.SNOWFLAKE

    # Status
    status: TableStatus = TableStatus.DRAFT
    is_deployed: bool = False
    last_deployed_at: Optional[datetime] = None

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def get_dimension_columns(self) -> List[DynamicTableColumn]:
        """Get columns marked as dimensions."""
        return [c for c in self.columns if c.is_dimension]

    def get_measure_columns(self) -> List[DynamicTableColumn]:
        """Get columns marked as measures."""
        return [c for c in self.columns if c.is_measure]

    def get_filters_by_precedence(self, precedence: int) -> List[FilterDefinition]:
        """Get filters for a specific precedence group."""
        return [f for f in self.filters if f.precedence_group == precedence]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()


# =============================================================================
# INTERMEDIATE AGGREGATION (DT_3A)
# =============================================================================


class IntermediateAggregation(BaseModel):
    """
    DT_3A: Intermediate aggregation with precedence handling.

    Used to aggregate data with filter precedence (precedence=1 gets priority).
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    dynamic_table_id: str  # Reference to DT_2

    # Hierarchy reference (from Librarian)
    hierarchy_id: Optional[str] = None
    hierarchy_name: Optional[str] = None

    # Precedence configuration
    precedence_groups: List[int] = Field(default_factory=lambda: [1])
    default_precedence: int = 1

    # Dimension filters per precedence
    # Format: {1: [{"column": "type", "values": ["A", "B"]}], 2: [...]}
    dimension_filters: Dict[int, List[FilterDefinition]] = Field(default_factory=dict)

    # Measures to aggregate
    measures: List[AggregationDefinition] = Field(default_factory=list)

    # Dimensions for grouping
    dimensions: List[str] = Field(default_factory=list)

    # Generated SQL
    generated_sql: Optional[str] = None

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def add_precedence_filter(
        self,
        precedence: int,
        column: str,
        values: List[Any],
        operator: str = "IN"
    ) -> None:
        """Add a filter for a precedence group."""
        if precedence not in self.dimension_filters:
            self.dimension_filters[precedence] = []
            if precedence not in self.precedence_groups:
                self.precedence_groups.append(precedence)
                self.precedence_groups.sort()

        self.dimension_filters[precedence].append(
            FilterDefinition(
                column=column,
                operator=operator,
                values=values,
                precedence_group=precedence
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()


# =============================================================================
# OUTPUT TABLE (DT_3)
# =============================================================================


class OutputTable(BaseModel):
    """
    DT_3: Final output table with formula calculations.

    Combines data from multiple sources and applies formulas.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_id: str
    table_name: str
    display_name: Optional[str] = None
    description: Optional[str] = None

    # Sources (DT_2 or DT_3A tables)
    source_tables: List[Dict[str, Any]] = Field(default_factory=list)
    # Format: [{"name": "DT_REVENUE", "alias": "r", "database": "...", "schema": "..."}]

    # Formula group (from Librarian)
    formula_group_id: Optional[str] = None
    formula_group_name: Optional[str] = None

    # Output structure
    dimensions: List[DynamicTableColumn] = Field(default_factory=list)
    base_measures: List[DynamicTableColumn] = Field(default_factory=list)
    calculated_columns: List[FormulaColumn] = Field(default_factory=list)

    # Joins between source tables
    source_joins: List[Dict[str, Any]] = Field(default_factory=list)
    # Format: [{"left": "r", "right": "b", "on": "r.period = b.period AND r.account = b.account"}]

    # Target
    target_database: Optional[str] = None
    target_schema: Optional[str] = None
    target_lag: str = "1 hour"
    warehouse: str = "COMPUTE_WH"

    # Generated SQL
    generated_sql: Optional[str] = None
    dialect: SQLDialect = SQLDialect.SNOWFLAKE

    # Deployment
    status: TableStatus = TableStatus.DRAFT
    deployment_connection_id: Optional[str] = None
    is_deployed: bool = False
    last_deployed_at: Optional[datetime] = None

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def add_source_table(
        self,
        name: str,
        alias: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        join_type: str = "LEFT",
        join_on: Optional[str] = None
    ) -> None:
        """Add a source table to the output."""
        source = {
            "name": name,
            "alias": alias,
            "database": database,
            "schema": schema,
            "join_type": join_type,
            "join_on": join_on
        }
        self.source_tables.append(source)

    def add_formula_column(
        self,
        alias: str,
        formula_type: FormulaType,
        operands: List[str],
        expression: Optional[str] = None,
        round_decimals: Optional[int] = None
    ) -> None:
        """Add a calculated column with formula."""
        self.calculated_columns.append(
            FormulaColumn(
                alias=alias,
                formula_type=formula_type,
                operands=operands,
                expression=expression,
                round_decimals=round_decimals
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()


# =============================================================================
# WORKFLOW DEFINITION
# =============================================================================


class SQLGenerationWorkflow(BaseModel):
    """
    End-to-end SQL generation workflow definition.

    Defines the complete flow from source to output.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_id: str
    name: str
    description: Optional[str] = None

    # Steps
    steps: List[Dict[str, Any]] = Field(default_factory=list)
    # Format: [{"type": "create_view", "config": {...}}, {"type": "create_dt", ...}]

    # Execution state
    current_step: int = 0
    status: str = "pending"  # pending, running, completed, failed
    error_message: Optional[str] = None

    # Results
    generated_objects: List[Dict[str, str]] = Field(default_factory=list)
    # Format: [{"type": "view", "name": "VW_REVENUE", "sql": "..."}]

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def add_step(
        self,
        step_type: str,
        config: Dict[str, Any],
        depends_on: Optional[List[int]] = None
    ) -> int:
        """Add a step to the workflow."""
        step_index = len(self.steps)
        self.steps.append({
            "index": step_index,
            "type": step_type,
            "config": config,
            "depends_on": depends_on or [],
            "status": "pending",
            "result": None
        })
        return step_index

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.model_dump()
