"""
Data Mart Factory Types.

Pydantic models for the Hierarchy-Driven Data Mart Factory:
- MartConfig: 7-variable configuration for pipeline generation
- JoinPattern: UNION ALL branch definitions
- DynamicColumnMapping: ID_SOURCE to physical column mapping
- PipelineObject: Generated DDL objects
- FormulaPrecedence: 5-level formula cascade definitions
- DiscoveryResult: AI-powered hierarchy analysis results
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class PipelineLayer(str, Enum):
    """Pipeline layer identifiers."""
    VW_1 = "VW_1"  # Translation View
    DT_2 = "DT_2"  # Granularity Table
    DT_3A = "DT_3A"  # Pre-Aggregation Fact
    DT_3 = "DT_3"  # Data Mart


class ObjectType(str, Enum):
    """Snowflake object types."""
    VIEW = "VIEW"
    DYNAMIC_TABLE = "DYNAMIC_TABLE"
    TABLE = "TABLE"


class FormulaLogic(str, Enum):
    """Formula calculation logic types."""
    SUM = "SUM"
    SUBTRACT = "SUBTRACT"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    AVERAGE = "AVERAGE"


class JoinPattern(BaseModel):
    """
    A single UNION ALL branch definition.

    Each branch in DT_3A represents a distinct join pattern
    between the hierarchy metadata (DT_2) and fact table.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str  # e.g., "account", "deduct_product", "royalty"
    description: Optional[str] = None
    join_keys: List[str]  # DT_2 columns: ["LOS_ACCOUNT_ID_FILTER"]
    fact_keys: List[str]  # Fact columns: ["FK_ACCOUNT_KEY"]
    filter: Optional[str] = None  # Optional WHERE: "ROYALTY_FILTER = 'Y'"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "join_keys": self.join_keys,
            "fact_keys": self.fact_keys,
            "filter": self.filter,
        }


class DynamicColumnMapping(BaseModel):
    """
    Maps ID_SOURCE to physical dimension column.

    The Translation View (VW_1) uses these mappings to convert
    abstract ID_SOURCE values to actual database column references.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    id_source: str  # "BILLING_CATEGORY_CODE"
    physical_column: str  # "ACCT.ACCOUNT_BILLING_CATEGORY_CODE"
    dimension_table: Optional[str] = None  # "DIM_ACCOUNT"
    is_alias: bool = False  # True for typo corrections

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "id_source": self.id_source,
            "physical_column": self.physical_column,
            "dimension_table": self.dimension_table,
            "is_alias": self.is_alias,
        }


class MartConfig(BaseModel):
    """
    Complete configuration for a data mart pipeline.

    Contains the 7 configuration variables that fully parameterize
    the pipeline generation for any hierarchy type.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    project_name: str
    description: Optional[str] = None

    # Source tables
    report_type: str  # "GROSS" or "NET"
    hierarchy_table: str  # Fully qualified table name
    mapping_table: str  # Fully qualified mapping table name
    fact_table: Optional[str] = None  # Fact table for joins

    # 7 Configuration Variables
    account_segment: str  # Filter value: "GROSS" or "NET"
    measure_prefix: Optional[str] = None  # Column prefix (defaults to report_type)
    has_sign_change: bool = False  # Apply sign flip logic
    has_exclusions: bool = False  # Generate NOT IN subqueries
    has_group_filter_precedence: bool = False  # Multi-round filtering

    # Complex configurations
    dynamic_column_map: List[DynamicColumnMapping] = Field(default_factory=list)
    join_patterns: List[JoinPattern] = Field(default_factory=list)

    # Target schema
    target_database: Optional[str] = None
    target_schema: Optional[str] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    @property
    def effective_measure_prefix(self) -> str:
        """Get the effective measure prefix."""
        return self.measure_prefix or self.report_type

    def add_join_pattern(self, pattern: JoinPattern) -> None:
        """Add a join pattern."""
        self.join_patterns.append(pattern)
        self.updated_at = datetime.now()

    def add_column_mapping(self, mapping: DynamicColumnMapping) -> None:
        """Add a column mapping."""
        self.dynamic_column_map.append(mapping)
        self.updated_at = datetime.now()

    def get_column_mapping(self, id_source: str) -> Optional[DynamicColumnMapping]:
        """Get mapping for an ID_SOURCE value."""
        for mapping in self.dynamic_column_map:
            if mapping.id_source == id_source:
                return mapping
        return None

    def to_yaml_dict(self) -> Dict[str, Any]:
        """Convert to dbt YAML-compatible dictionary."""
        return {
            "project_name": self.project_name,
            "report_type": self.report_type,
            "hierarchy_table": self.hierarchy_table,
            "mapping_table": self.mapping_table,
            "account_segment": self.account_segment,
            "measure_prefix": self.effective_measure_prefix,
            "has_sign_change": self.has_sign_change,
            "has_exclusions": self.has_exclusions,
            "has_group_filter_precedence": self.has_group_filter_precedence,
            "dynamic_column_map": {
                m.id_source: m.physical_column
                for m in self.dynamic_column_map
            },
            "join_patterns": [
                {
                    "name": p.name,
                    "join_keys": p.join_keys,
                    "fact_keys": p.fact_keys,
                    "filter": p.filter,
                }
                for p in self.join_patterns
            ],
        }

    def to_summary(self) -> Dict[str, Any]:
        """Get configuration summary."""
        return {
            "id": self.id,
            "project_name": self.project_name,
            "report_type": self.report_type,
            "account_segment": self.account_segment,
            "join_pattern_count": len(self.join_patterns),
            "column_mapping_count": len(self.dynamic_column_map),
            "has_sign_change": self.has_sign_change,
            "has_exclusions": self.has_exclusions,
            "has_group_filter_precedence": self.has_group_filter_precedence,
            "created_at": self.created_at.isoformat(),
        }


class PipelineObject(BaseModel):
    """
    A generated DDL object in the pipeline.

    Represents one of the 4 pipeline objects:
    VW_1, DT_2, DT_3A, or DT_3.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    object_type: ObjectType
    object_name: str  # "VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED"
    layer: PipelineLayer
    layer_order: int  # 1, 2, 3, 4

    # Generated content
    ddl: str  # Full DDL statement
    description: Optional[str] = None

    # Metadata
    estimated_rows: Optional[int] = None
    dependencies: List[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "object_type": self.object_type.value,
            "object_name": self.object_name,
            "layer": self.layer.value,
            "layer_order": self.layer_order,
            "description": self.description,
            "estimated_rows": self.estimated_rows,
            "dependencies": self.dependencies,
            "ddl_length": len(self.ddl),
        }


class FormulaPrecedence(BaseModel):
    """
    A formula calculation definition.

    Part of the 5-level formula precedence cascade that computes
    calculated rows in DT_3.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    precedence_level: int  # 1-5
    formula_group: str  # "GROSS_PROFIT", "OPERATING_INCOME"
    hierarchy_key: Optional[str] = None  # FK_REPORT_KEY value

    # Calculation definition
    logic: FormulaLogic  # SUM, SUBTRACT, etc.
    param_ref: str  # "Total Revenue"
    param2_ref: Optional[str] = None  # "Total Taxes and Deducts"

    # Additional params for complex formulas
    additional_params: List[str] = Field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "precedence_level": self.precedence_level,
            "formula_group": self.formula_group,
            "hierarchy_key": self.hierarchy_key,
            "logic": self.logic.value,
            "param_ref": self.param_ref,
            "param2_ref": self.param2_ref,
            "additional_params": self.additional_params,
        }


class DataQualityIssue(BaseModel):
    """A detected data quality issue."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    severity: str  # "HIGH", "MEDIUM", "LOW"
    issue_type: str  # "TYPO", "ORPHAN", "DUPLICATE", "MISMATCH"
    description: str
    affected_rows: int = 0
    affected_values: List[str] = Field(default_factory=list)
    recommendation: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "severity": self.severity,
            "issue_type": self.issue_type,
            "description": self.description,
            "affected_rows": self.affected_rows,
            "affected_values": self.affected_values[:10],  # Limit display
            "recommendation": self.recommendation,
        }


class DiscoveryResult(BaseModel):
    """
    Result of AI-powered hierarchy discovery.

    Contains the analysis results from scanning hierarchy
    and mapping tables to detect patterns and issues.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Source analysis
    hierarchy_table: str
    mapping_table: str
    hierarchy_type: Optional[str] = None  # Detected type: "P&L", "LOS", etc.

    # Structure analysis
    level_count: int = 0
    node_count: int = 0
    mapping_count: int = 0
    active_node_count: int = 0
    calculation_node_count: int = 0

    # ID_SOURCE distribution
    id_source_distribution: Dict[str, int] = Field(default_factory=dict)
    id_table_distribution: Dict[str, int] = Field(default_factory=dict)

    # Suggestions
    join_pattern_suggestion: List[JoinPattern] = Field(default_factory=list)
    column_map_suggestion: List[DynamicColumnMapping] = Field(default_factory=list)

    # Data quality
    data_quality_issues: List[DataQualityIssue] = Field(default_factory=list)

    # Recommended configuration
    recommended_config: Optional[MartConfig] = None

    # AI explanation
    explanation: Optional[str] = None
    confidence_score: float = 0.0

    # Metadata
    discovered_at: datetime = Field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "hierarchy_table": self.hierarchy_table,
            "mapping_table": self.mapping_table,
            "hierarchy_type": self.hierarchy_type,
            "level_count": self.level_count,
            "node_count": self.node_count,
            "mapping_count": self.mapping_count,
            "active_node_count": self.active_node_count,
            "calculation_node_count": self.calculation_node_count,
            "id_source_distribution": self.id_source_distribution,
            "join_pattern_count": len(self.join_pattern_suggestion),
            "column_mapping_count": len(self.column_map_suggestion),
            "data_quality_issue_count": len(self.data_quality_issues),
            "has_recommended_config": self.recommended_config is not None,
            "confidence_score": self.confidence_score,
            "discovered_at": self.discovered_at.isoformat(),
        }


class PipelineValidationResult(BaseModel):
    """Result of pipeline validation against source data."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    config_name: str

    # Validation status
    is_valid: bool = True

    # Per-layer results
    layer_results: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Row count validation
    expected_rows: Dict[str, int] = Field(default_factory=dict)
    actual_rows: Dict[str, int] = Field(default_factory=dict)
    row_count_match: bool = True

    # Issues found
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)

    # Metadata
    validated_at: datetime = Field(default_factory=datetime.now)
    duration_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "config_name": self.config_name,
            "is_valid": self.is_valid,
            "layer_results": self.layer_results,
            "row_count_match": self.row_count_match,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "validated_at": self.validated_at.isoformat(),
            "duration_seconds": self.duration_seconds,
        }
