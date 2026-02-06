"""Pydantic models for Faux Objects - Semantic View wrapper generation."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class FauxObjectType(str, Enum):
    """Types of standard Snowflake objects that can wrap a Semantic View."""
    VIEW = "view"
    STORED_PROCEDURE = "stored_procedure"
    DYNAMIC_TABLE = "dynamic_table"
    TASK = "task"


class SemanticColumnType(str, Enum):
    """Column classification in a Semantic View."""
    DIMENSION = "dimension"
    METRIC = "metric"
    FACT = "fact"


class SnowflakeDataType(str, Enum):
    """Common Snowflake data types for column definitions."""
    VARCHAR = "VARCHAR"
    INT = "INT"
    FLOAT = "FLOAT"
    NUMBER = "NUMBER"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP_NTZ"
    VARIANT = "VARIANT"


class SemanticColumn(BaseModel):
    """A column (dimension, metric, or fact) in a Semantic View."""
    name: str = Field(..., description="Column name as defined in the semantic view")
    column_type: SemanticColumnType = Field(..., description="Whether this is a dimension, metric, or fact")
    data_type: str = Field("VARCHAR", description="Snowflake data type (VARCHAR, FLOAT, INT, etc.)")
    table_alias: Optional[str] = Field(None, description="Table alias prefix (e.g., 'accounts' in accounts.account_name)")
    expression: Optional[str] = Field(None, description="SQL expression for metrics (e.g., SUM(net_amount))")
    synonyms: List[str] = Field(default_factory=list, description="Business-friendly synonyms")
    comment: Optional[str] = Field(None, description="Column description")

    @property
    def qualified_name(self) -> str:
        """Full qualified name with table alias if present."""
        if self.table_alias:
            return f"{self.table_alias}.{self.name}"
        return self.name


class SemanticRelationship(BaseModel):
    """A relationship between tables in a Semantic View."""
    from_table: str = Field(..., description="Source table alias")
    from_column: str = Field(..., description="Source column name")
    to_table: str = Field(..., description="Target table alias")
    to_column: Optional[str] = Field(None, description="Target column (defaults to primary key)")


class SemanticTable(BaseModel):
    """A table referenced by a Semantic View."""
    alias: str = Field(..., description="Table alias used in the semantic view")
    fully_qualified_name: str = Field(..., description="Full path: DATABASE.SCHEMA.TABLE")
    primary_key: Optional[str] = Field(None, description="Primary key column")


class ProcedureParameter(BaseModel):
    """A parameter for a stored procedure faux object."""
    name: str = Field(..., description="Parameter name")
    data_type: str = Field("VARCHAR", description="Snowflake data type")
    default_value: Optional[str] = Field(None, description="Default value (SQL literal)")
    description: Optional[str] = Field(None, description="Parameter description")


class SemanticViewDefinition(BaseModel):
    """Complete definition of a Snowflake Semantic View."""
    name: str = Field(..., description="Semantic view name")
    database: str = Field(..., description="Database containing the semantic view")
    schema_name: str = Field(..., description="Schema containing the semantic view")
    comment: Optional[str] = Field(None, description="Semantic view description")
    ai_sql_generation: Optional[str] = Field(None, description="AI context hint for Cortex Analyst")
    tables: List[SemanticTable] = Field(default_factory=list, description="Referenced tables")
    relationships: List[SemanticRelationship] = Field(default_factory=list, description="Table relationships")
    dimensions: List[SemanticColumn] = Field(default_factory=list, description="Dimension columns")
    metrics: List[SemanticColumn] = Field(default_factory=list, description="Metric columns")
    facts: List[SemanticColumn] = Field(default_factory=list, description="Fact columns")

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.name}"

    def get_all_columns(self) -> List[SemanticColumn]:
        return self.dimensions + self.metrics + self.facts


class FauxObjectConfig(BaseModel):
    """Configuration for a single faux object to generate."""
    name: str = Field(..., description="Name of the faux object (view/procedure/table name)")
    faux_type: FauxObjectType = Field(..., description="Type of faux object")
    target_database: str = Field(..., description="Target database for deployment")
    target_schema: str = Field(..., description="Target schema for deployment")
    selected_dimensions: List[str] = Field(default_factory=list, description="Dimension names to include")
    selected_metrics: List[str] = Field(default_factory=list, description="Metric names to include")
    selected_facts: List[str] = Field(default_factory=list, description="Fact names to include")
    parameters: List[ProcedureParameter] = Field(default_factory=list, description="Procedure parameters (stored_procedure type only)")
    warehouse: Optional[str] = Field(None, description="Warehouse for dynamic tables/tasks")
    target_lag: Optional[str] = Field(None, description="Target lag for dynamic tables (e.g., '2 hours')")
    schedule: Optional[str] = Field(None, description="CRON schedule for tasks")
    materialized_table: Optional[str] = Field(None, description="Target table name for task materialization")
    where_clause: Optional[str] = Field(None, description="Static WHERE clause filter")
    comment: Optional[str] = Field(None, description="Object description")

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.target_database}.{self.target_schema}.{self.name}"


class FauxProject(BaseModel):
    """A Faux Objects project containing a semantic view definition and its wrapper objects."""
    id: str = Field(..., description="Unique project identifier")
    name: str = Field(..., description="Project name")
    description: Optional[str] = Field(None, description="Project description")
    semantic_view: Optional[SemanticViewDefinition] = Field(None, description="Source semantic view definition")
    faux_objects: List[FauxObjectConfig] = Field(default_factory=list, description="Configured faux objects")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        extra = "allow"


class GeneratedScript(BaseModel):
    """A generated SQL script."""
    object_name: str = Field(..., description="Name of the generated object")
    object_type: FauxObjectType = Field(..., description="Type of the generated object")
    sql: str = Field(..., description="The generated SQL script")
    dependencies: List[str] = Field(default_factory=list, description="Objects this depends on")
