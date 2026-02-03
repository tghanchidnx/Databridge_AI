"""Pydantic models for Hierarchy Builder data structures."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class FormatTier(str, Enum):
    """Hierarchy format complexity tiers for flexible import."""
    TIER_1 = "tier_1"  # Ultra-simple: 2-3 columns (source_value, group_name)
    TIER_2 = "tier_2"  # Basic: 5-7 columns with parent names
    TIER_3 = "tier_3"  # Standard: 10-12 columns with explicit IDs
    TIER_4 = "tier_4"  # Enterprise: 28+ columns with LEVEL_X


class InputFormat(str, Enum):
    """Supported input formats for flexible import."""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    TEXT = "text"
    AUTO = "auto"


class ProjectDefaults(BaseModel):
    """Default source information for a project (used in flexible import)."""
    source_database: str = Field(default="", description="Default source database name")
    source_schema: str = Field(default="", description="Default source schema name")
    source_table: str = Field(default="", description="Default source table name")
    source_column: str = Field(default="", description="Default source column name")

    def is_complete(self) -> bool:
        """Check if all source defaults are set."""
        return all([
            self.source_database,
            self.source_schema,
            self.source_table,
            self.source_column,
        ])

    class Config:
        extra = "allow"


class PropertyCategory(str, Enum):
    """Categories of hierarchy properties."""
    DIMENSION = "dimension"      # Controls dimension building (aggregation, display, drill)
    FACT = "fact"                # Controls fact/measure design (measure type, calculation)
    FILTER = "filter"            # Controls filter behavior (default values, cascading)
    DISPLAY = "display"          # Controls UI display (color, icon, format)
    VALIDATION = "validation"    # Data validation rules
    SECURITY = "security"        # Row-level security, access control
    CUSTOM = "custom"            # User-defined properties


class AggregationType(str, Enum):
    """Aggregation types for dimensions and facts."""
    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MIN = "MIN"
    MAX = "MAX"
    FIRST = "FIRST"
    LAST = "LAST"
    NONE = "NONE"              # No aggregation (display only)
    SEMI_ADDITIVE = "SEMI_ADDITIVE"  # For balance-type measures


class MeasureType(str, Enum):
    """Types of measures for fact properties."""
    ADDITIVE = "additive"           # Can be summed across all dimensions
    SEMI_ADDITIVE = "semi_additive" # Cannot be summed across time (e.g., balances)
    NON_ADDITIVE = "non_additive"   # Cannot be summed (e.g., ratios, percentages)
    DERIVED = "derived"             # Calculated from other measures
    SNAPSHOT = "snapshot"           # Point-in-time values


class TimeBalance(str, Enum):
    """Time balance behavior for semi-additive measures."""
    FLOW = "flow"                 # Sum over time (income, expenses)
    BALANCE_FIRST = "first"       # First value in period
    BALANCE_LAST = "last"         # Last value in period (typical for balances)
    BALANCE_AVG = "average"       # Average over period


class FilterBehavior(str, Enum):
    """Filter behavior types."""
    SINGLE_SELECT = "single"      # Single value selection
    MULTI_SELECT = "multi"        # Multiple value selection
    RANGE = "range"               # Range selection (dates, numbers)
    CASCADING = "cascading"       # Dependent on parent filter
    SEARCH = "search"             # Free-text search
    HIERARCHY = "hierarchy"       # Tree-based selection


class HierarchyProperty(BaseModel):
    """
    A property attached to a hierarchy node.

    Properties control how dimensions are built, facts are designed,
    and filters are configured. Properties can be inherited by children.
    """
    name: str = Field(..., description="Property name (e.g., 'aggregation_type', 'measure_type')")
    value: Any = Field(..., description="Property value")
    category: PropertyCategory = Field(PropertyCategory.CUSTOM, description="Property category")
    level: Optional[int] = Field(None, description="Specific level this applies to (None = hierarchy level)")
    inherit: bool = Field(True, description="Whether child hierarchies inherit this property")
    override_allowed: bool = Field(True, description="Whether children can override this property")
    description: Optional[str] = Field(None, description="Property description")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    class Config:
        extra = "allow"


class DimensionProperties(BaseModel):
    """Pre-defined dimension property set."""
    aggregation_type: AggregationType = Field(AggregationType.SUM, description="How to aggregate")
    display_format: Optional[str] = Field(None, description="Display format string")
    sort_behavior: str = Field("alpha", description="Sort: alpha, numeric, custom, natural")
    drill_enabled: bool = Field(True, description="Allow drill-down")
    drill_path: Optional[List[str]] = Field(None, description="Custom drill path hierarchy IDs")
    grouping_enabled: bool = Field(True, description="Allow grouping in reports")
    totals_enabled: bool = Field(True, description="Show totals for this dimension")
    hierarchy_type: str = Field("standard", description="standard, ragged, parent-child, time")
    all_member_name: Optional[str] = Field(None, description="Name for 'All' member")
    default_member: Optional[str] = Field(None, description="Default member ID for queries")

    class Config:
        extra = "allow"


class FactProperties(BaseModel):
    """Pre-defined fact/measure property set."""
    measure_type: MeasureType = Field(MeasureType.ADDITIVE, description="Measure type")
    aggregation_type: AggregationType = Field(AggregationType.SUM, description="Default aggregation")
    time_balance: Optional[TimeBalance] = Field(None, description="Time balance behavior")
    format_string: Optional[str] = Field(None, description="Number format (e.g., '#,##0.00')")
    decimal_places: int = Field(2, description="Decimal places for display")
    currency_code: Optional[str] = Field(None, description="Currency code (e.g., 'USD')")
    unit_of_measure: Optional[str] = Field(None, description="Unit (e.g., 'bbl', 'mcf', 'units')")
    null_handling: str = Field("zero", description="How to handle nulls: zero, null, exclude")
    negative_format: str = Field("minus", description="Negative display: minus, parens, red")
    calculation_formula: Optional[str] = Field(None, description="SQL/expression for derived measures")
    base_measure_ids: Optional[List[str]] = Field(None, description="IDs of measures used in calculation")

    class Config:
        extra = "allow"


class FilterProperties(BaseModel):
    """Pre-defined filter property set."""
    filter_behavior: FilterBehavior = Field(FilterBehavior.MULTI_SELECT, description="Filter type")
    default_value: Optional[Any] = Field(None, description="Default filter value")
    default_to_all: bool = Field(True, description="Default to all values if no selection")
    allowed_values: Optional[List[Any]] = Field(None, description="Restrict to these values")
    excluded_values: Optional[List[Any]] = Field(None, description="Exclude these values")
    cascading_parent_id: Optional[str] = Field(None, description="Parent filter hierarchy ID")
    required: bool = Field(False, description="Filter selection required")
    visible: bool = Field(True, description="Show in filter panel")
    search_enabled: bool = Field(True, description="Enable search in filter")
    show_all_option: bool = Field(True, description="Show 'All' option")
    max_selections: Optional[int] = Field(None, description="Max selections for multi-select")

    class Config:
        extra = "allow"


class DisplayProperties(BaseModel):
    """Pre-defined display property set."""
    color: Optional[str] = Field(None, description="Display color (hex or name)")
    background_color: Optional[str] = Field(None, description="Background color")
    icon: Optional[str] = Field(None, description="Icon name or emoji")
    tooltip: Optional[str] = Field(None, description="Hover tooltip text")
    visible: bool = Field(True, description="Visible in UI")
    collapsed_by_default: bool = Field(False, description="Start collapsed in tree views")
    highlight_condition: Optional[str] = Field(None, description="Condition for highlighting")
    custom_css_class: Optional[str] = Field(None, description="Custom CSS class")
    display_order: Optional[int] = Field(None, description="Override display order")

    class Config:
        extra = "allow"


class PropertyTemplate(BaseModel):
    """
    Pre-defined property template for common configurations.

    Templates allow quick application of standard property sets
    to hierarchies (e.g., 'Financial Dimension', 'Time Dimension', 'Measure').
    """
    id: str = Field(..., description="Template ID")
    name: str = Field(..., description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    category: PropertyCategory = Field(..., description="Primary category")
    properties: List[HierarchyProperty] = Field(default_factory=list, description="Properties in template")
    dimension_props: Optional[DimensionProperties] = Field(None, description="Dimension properties")
    fact_props: Optional[FactProperties] = Field(None, description="Fact properties")
    filter_props: Optional[FilterProperties] = Field(None, description="Filter properties")
    display_props: Optional[DisplayProperties] = Field(None, description="Display properties")
    applicable_to: List[str] = Field(default_factory=list, description="Hierarchy types this applies to")
    tags: List[str] = Field(default_factory=list, description="Tags for searching")

    class Config:
        extra = "allow"


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
    # Property system - controls dimensions, facts, filters
    properties: List["HierarchyProperty"] = Field(default_factory=list, description="Custom properties")
    dimension_props: Optional["DimensionProperties"] = Field(None, description="Dimension configuration")
    fact_props: Optional["FactProperties"] = Field(None, description="Fact/measure configuration")
    filter_props: Optional["FilterProperties"] = Field(None, description="Filter configuration")
    display_props: Optional["DisplayProperties"] = Field(None, description="Display configuration")
    property_template_id: Optional[str] = Field(None, description="Applied property template ID")
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
