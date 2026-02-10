"""Pydantic models for Hierarchy Builder data structures."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class FormulaOperation(str, Enum):
    """Supported formula operations."""
    SUM = "SUM"
    SUBTRACT = "SUBTRACT"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    AVERAGE = "AVERAGE"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"


class HierarchyFlags(BaseModel):
    """Flags controlling hierarchy behavior."""
    include_flag: bool = True
    exclude_flag: bool = False
    transform_flag: bool = False
    calculation_flag: bool = False
    active_flag: bool = True
    is_leaf_node: bool = False
    custom_flags: Optional[Dict[str, bool]] = None

    class Config:
        extra = "allow"


class SourceMappingFlags(BaseModel):
    """Flags for source mapping entries."""
    include_flag: bool = True
    exclude_flag: bool = False
    transform_flag: bool = False
    active_flag: bool = True
    custom_flags: Optional[Dict[str, bool]] = None


class SourceMapping(BaseModel):
    """Source database mapping for a hierarchy node."""
    mapping_index: int = Field(..., description="Order/precedence of this mapping")
    source_database: str = Field(..., description="Source database name")
    source_schema: str = Field(..., description="Source schema name")
    source_table: str = Field(..., description="Source table name")
    source_column: str = Field(..., description="Source column name")
    source_uid: Optional[str] = Field(None, description="Specific value to match")
    precedence_group: Optional[str] = Field("1", description="Precedence grouping")
    flags: SourceMappingFlags = Field(default_factory=SourceMappingFlags)


class FormulaRule(BaseModel):
    """A single rule in a formula group."""
    operation: FormulaOperation = Field(..., description="Mathematical operation")
    hierarchy_id: str = Field(..., description="Source hierarchy ID")
    hierarchy_name: str = Field(..., description="Source hierarchy name")
    precedence: int = Field(1, description="Order of operations")
    param_ref: Optional[str] = Field(None, description="Parameter reference")
    constant_number: Optional[float] = Field(None, description="Constant multiplier/divisor")


class FormulaGroup(BaseModel):
    """Formula group for calculated hierarchies."""
    group_name: str = Field(..., description="Name of the formula group")
    main_hierarchy_id: str = Field(..., description="Hierarchy storing the result")
    main_hierarchy_name: str = Field(..., description="Name of main hierarchy")
    rules: List[FormulaRule] = Field(default_factory=list)
    formula_params: Optional[Dict[str, Any]] = None


class FilterCondition(BaseModel):
    """A single filter condition."""
    column: str = Field(..., description="Column to filter on")
    operator: str = Field(..., description="Comparison operator")
    value: Any = Field(..., description="Value to compare")
    logic: Optional[str] = Field("AND", description="AND/OR logic")


class FilterGroup(BaseModel):
    """Reusable filter group configuration."""
    id: Optional[str] = None
    group_name: str = Field(..., description="Filter group name")
    filter_group_1: Optional[str] = None
    filter_group_2: Optional[str] = None
    filter_group_3: Optional[str] = None
    filter_group_4: Optional[str] = None
    filter_conditions: List[FilterCondition] = Field(default_factory=list)
    custom_sql: Optional[str] = None


class HierarchyLevel(BaseModel):
    """Hierarchy level structure (up to 15 levels) with optional sort orders."""
    # Level values
    level_1: Optional[str] = None
    level_2: Optional[str] = None
    level_3: Optional[str] = None
    level_4: Optional[str] = None
    level_5: Optional[str] = None
    level_6: Optional[str] = None
    level_7: Optional[str] = None
    level_8: Optional[str] = None
    level_9: Optional[str] = None
    level_10: Optional[str] = None
    level_11: Optional[str] = None
    level_12: Optional[str] = None
    level_13: Optional[str] = None
    level_14: Optional[str] = None
    level_15: Optional[str] = None

    # Level sort orders (optional, for controlling display order within each level)
    level_1_sort: Optional[int] = None
    level_2_sort: Optional[int] = None
    level_3_sort: Optional[int] = None
    level_4_sort: Optional[int] = None
    level_5_sort: Optional[int] = None
    level_6_sort: Optional[int] = None
    level_7_sort: Optional[int] = None
    level_8_sort: Optional[int] = None
    level_9_sort: Optional[int] = None
    level_10_sort: Optional[int] = None
    level_11_sort: Optional[int] = None
    level_12_sort: Optional[int] = None
    level_13_sort: Optional[int] = None
    level_14_sort: Optional[int] = None
    level_15_sort: Optional[int] = None

    def to_list(self) -> List[str]:
        """Convert to list of non-empty levels."""
        levels = []
        for i in range(1, 16):
            val = getattr(self, f"level_{i}", None)
            if val:
                levels.append(val)
            else:
                break
        return levels

    def to_sort_list(self) -> List[Optional[int]]:
        """Convert to list of sort values for non-empty levels."""
        sorts = []
        for i in range(1, 16):
            level_val = getattr(self, f"level_{i}", None)
            if level_val:
                sorts.append(getattr(self, f"level_{i}_sort", None))
            else:
                break
        return sorts

    def depth(self) -> int:
        """Get the depth of the hierarchy."""
        return len(self.to_list())


class FormulaConfig(BaseModel):
    """Formula configuration for a hierarchy."""
    formula_type: str = Field("EXPRESSION", description="SQL, EXPRESSION, or AGGREGATE")
    formula_text: Optional[str] = None
    formula_group: Optional[FormulaGroup] = None
    variables: Optional[Dict[str, Any]] = None


class FilterConfig(BaseModel):
    """Filter configuration for a hierarchy."""
    filter_group_1: Optional[str] = None
    filter_group_2: Optional[str] = None
    filter_group_3: Optional[str] = None
    filter_group_4: Optional[str] = None
    filter_conditions: List[FilterCondition] = Field(default_factory=list)
    custom_sql: Optional[str] = None
    filter_group_ref: Optional[Dict[str, str]] = None


class SmartHierarchy(BaseModel):
    """Complete smart hierarchy master record."""
    id: Optional[str] = None
    project_id: str = Field(..., description="Parent project ID")
    hierarchy_id: str = Field(..., description="Unique hierarchy identifier")
    hierarchy_name: str = Field(..., description="Display name")
    description: Optional[str] = None
    parent_id: Optional[str] = Field(None, description="Parent hierarchy ID")
    is_root: bool = Field(False, description="Is root node")
    sort_order: int = Field(0, description="Display order")
    hierarchy_level: HierarchyLevel = Field(default_factory=HierarchyLevel)
    flags: HierarchyFlags = Field(default_factory=HierarchyFlags)
    mapping: List[SourceMapping] = Field(default_factory=list)
    formula_config: Optional[FormulaConfig] = None
    filter_config: Optional[FilterConfig] = None
    metadata: Optional[Dict[str, Any]] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        extra = "allow"


class HierarchyProject(BaseModel):
    """Hierarchy project container."""
    id: Optional[str] = None
    name: str = Field(..., description="Project name")
    description: Optional[str] = None
    deployment_config: Optional[Dict[str, Any]] = None
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        extra = "allow"


class DeploymentConfig(BaseModel):
    """Configuration for database deployment."""
    connection_id: str
    database: str
    schema_name: str
    master_table_name: str = "HIERARCHY_MASTER"
    master_view_name: str = "V_HIERARCHY_MASTER"
    create_tables: bool = True
    create_views: bool = True
    database_type: str = "snowflake"


class DeploymentHistory(BaseModel):
    """Record of a deployment."""
    id: Optional[str] = None
    project_id: str
    deployed_by: str
    deployment_config: DeploymentConfig
    scripts: Dict[str, str]
    status: str = "pending"
    error_message: Optional[str] = None
    deployed_at: Optional[datetime] = None
