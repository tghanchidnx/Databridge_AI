"""
Pydantic models and types for Cortex Analyst integration.

Cortex Analyst enables natural language to SQL translation using semantic models.
This module defines the data structures for:
- Semantic model configuration (tables, dimensions, metrics)
- Analyst API request/response types
- Multi-turn conversation tracking
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field
import uuid


# =============================================================================
# Semantic Model Types
# =============================================================================

class DataType(str, Enum):
    """Supported data types in semantic models."""
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    NUMBER = "NUMBER"
    FLOAT = "FLOAT"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    VARIANT = "VARIANT"


class JoinType(str, Enum):
    """Join types for table relationships."""
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"


class RelationshipType(str, Enum):
    """Cardinality of table relationships."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class BaseTableRef(BaseModel):
    """Reference to a base table in Snowflake."""
    database: str = Field(..., description="Database name")
    schema_name: str = Field(..., alias="schema", description="Schema name")
    table: str = Field(..., description="Table name")

    model_config = {"extra": "allow", "populate_by_name": True}

    def fully_qualified(self) -> str:
        """Return fully qualified table name."""
        return f"{self.database}.{self.schema_name}.{self.table}"


class Dimension(BaseModel):
    """A dimension column in a logical table."""
    name: str = Field(..., description="Dimension name")
    synonyms: List[str] = Field(default_factory=list, description="Alternative names")
    description: str = Field(..., description="Business description")
    expr: str = Field(..., description="SQL expression for this dimension")
    data_type: str = Field(default="VARCHAR", description="Data type")
    unique: bool = Field(default=False, description="Whether values are unique")
    sample_values: List[str] = Field(default_factory=list, description="Example values")

    model_config = {"extra": "allow"}


class TimeDimension(BaseModel):
    """A time/date dimension for temporal analysis."""
    name: str = Field(..., description="Time dimension name")
    synonyms: List[str] = Field(default_factory=list, description="Alternative names")
    description: str = Field(..., description="Business description")
    expr: str = Field(..., description="SQL expression")
    data_type: str = Field(default="DATE", description="Date/timestamp type")

    model_config = {"extra": "allow"}


class Metric(BaseModel):
    """An aggregated metric/measure."""
    name: str = Field(..., description="Metric name")
    synonyms: List[str] = Field(default_factory=list, description="Alternative names")
    description: str = Field(..., description="Business description")
    expr: str = Field(..., description="SQL aggregation expression (e.g., SUM(amount))")
    data_type: str = Field(default="NUMBER", description="Result data type")
    default_aggregation: str = Field(default="SUM", description="Default aggregation")

    model_config = {"extra": "allow"}


class Fact(BaseModel):
    """A raw fact column (non-aggregated measure)."""
    name: str = Field(..., description="Fact name")
    synonyms: List[str] = Field(default_factory=list, description="Alternative names")
    description: str = Field(..., description="Business description")
    expr: str = Field(..., description="SQL expression")
    data_type: str = Field(..., description="Data type")

    model_config = {"extra": "allow"}


class JoinColumn(BaseModel):
    """Column mapping for a join relationship."""
    left_column: str = Field(..., description="Column from left table")
    right_column: str = Field(..., description="Column from right table")

    model_config = {"extra": "allow"}


class TableRelationship(BaseModel):
    """Relationship between two logical tables."""
    left_table: str = Field(..., description="Name of left table")
    right_table: str = Field(..., description="Name of right table")
    join_type: JoinType = Field(default=JoinType.LEFT_OUTER)
    relationship_type: RelationshipType = Field(default=RelationshipType.MANY_TO_ONE)
    columns: List[JoinColumn] = Field(..., description="Join column mappings")

    model_config = {"extra": "allow"}


class LogicalTable(BaseModel):
    """A logical table in the semantic model."""
    name: str = Field(..., description="Logical table name")
    description: str = Field(..., description="Business description")
    base_table: BaseTableRef = Field(..., description="Physical table reference")
    dimensions: List[Dimension] = Field(default_factory=list)
    time_dimensions: List[TimeDimension] = Field(default_factory=list)
    metrics: List[Metric] = Field(default_factory=list)
    facts: List[Fact] = Field(default_factory=list)
    primary_key: Optional[str] = Field(default=None, description="Primary key column")

    model_config = {"extra": "allow"}


class SemanticModelConfig(BaseModel):
    """Configuration for a complete semantic model."""
    name: str = Field(..., description="Model name (used in file name)")
    description: str = Field(..., description="Model description")
    database: str = Field(..., description="Default database")
    schema_name: str = Field(..., alias="schema", description="Default schema")
    tables: List[LogicalTable] = Field(default_factory=list)
    relationships: List[TableRelationship] = Field(default_factory=list)
    version: str = Field(default="1.0.0", description="Model version")
    created_at: Optional[datetime] = Field(default=None)
    updated_at: Optional[datetime] = Field(default=None)

    model_config = {"extra": "allow", "populate_by_name": True}


# =============================================================================
# Analyst API Types
# =============================================================================

class MessageContentType(str, Enum):
    """Types of content in Analyst messages."""
    TEXT = "text"
    SQL = "sql"
    SUGGESTIONS = "suggestions"
    ERROR = "error"


class MessageContent(BaseModel):
    """Content block within an Analyst message."""
    type: MessageContentType = Field(..., description="Content type")
    text: Optional[str] = Field(default=None, description="Text content")
    statement: Optional[str] = Field(default=None, description="SQL statement")
    suggestions: Optional[List[str]] = Field(default=None, description="Follow-up suggestions")
    error_message: Optional[str] = Field(default=None, description="Error message if type=error")

    model_config = {"extra": "allow"}


class AnalystMessage(BaseModel):
    """A message in the Analyst conversation."""
    role: str = Field(..., description="Message role: 'user' or 'analyst'")
    content: List[MessageContent] = Field(default_factory=list)

    model_config = {"extra": "allow"}

    @classmethod
    def user_message(cls, question: str) -> "AnalystMessage":
        """Create a user message."""
        return cls(
            role="user",
            content=[MessageContent(type=MessageContentType.TEXT, text=question)]
        )


class AnalystResponse(BaseModel):
    """Response from the Cortex Analyst API."""
    request_id: str = Field(..., description="Unique request identifier")
    message: AnalystMessage = Field(..., description="Analyst response message")
    sql: Optional[str] = Field(default=None, description="Extracted SQL statement")
    explanation: Optional[str] = Field(default=None, description="Text explanation")
    suggestions: List[str] = Field(default_factory=list, description="Follow-up suggestions")
    success: bool = Field(default=True)
    error: Optional[str] = Field(default=None)

    model_config = {"extra": "allow"}


class AnalystConversation(BaseModel):
    """A multi-turn conversation with Cortex Analyst."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    semantic_model: str = Field(..., description="Semantic model file path or view name")
    messages: List[AnalystMessage] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)
    last_sql: Optional[str] = Field(default=None, description="Last generated SQL")

    model_config = {"extra": "allow"}

    def add_user_message(self, question: str) -> None:
        """Add a user question to the conversation."""
        self.messages.append(AnalystMessage.user_message(question))

    def add_analyst_response(self, response: AnalystResponse) -> None:
        """Add an analyst response to the conversation."""
        self.messages.append(response.message)
        if response.sql:
            self.last_sql = response.sql

    def get_messages_for_api(self) -> List[Dict[str, Any]]:
        """Get messages in API format for multi-turn."""
        return [
            {"role": m.role, "content": [c.model_dump(exclude_none=True) for c in m.content]}
            for m in self.messages
        ]


# =============================================================================
# Query Execution Types
# =============================================================================

class QueryResult(BaseModel):
    """Result of executing generated SQL."""
    sql: str = Field(..., description="The SQL that was executed")
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    columns: List[str] = Field(default_factory=list)
    row_count: int = Field(default=0)
    execution_time_ms: int = Field(default=0)
    truncated: bool = Field(default=False, description="Whether results were truncated")
    error: Optional[str] = Field(default=None)

    model_config = {"extra": "allow"}


class AnalystQueryResult(BaseModel):
    """Combined result from Analyst + query execution."""
    question: str = Field(..., description="Original natural language question")
    explanation: Optional[str] = Field(default=None)
    sql: str = Field(..., description="Generated SQL")
    results: QueryResult = Field(..., description="Query execution results")
    suggestions: List[str] = Field(default_factory=list)
    conversation_id: Optional[str] = Field(default=None)
    success: bool = Field(default=True)
    error: Optional[str] = Field(default=None)

    model_config = {"extra": "allow"}
