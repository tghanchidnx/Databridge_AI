"""
Hierarchy Types for DataBridge AI Librarian.

Defines hierarchy type classifications and their associated behaviors:
- STANDARD: Basic hierarchical grouping
- GROUPING: Aggregation/rollup hierarchy with business logic
- XREF: Cross-reference hierarchy for dimensional linking
- CALCULATION: Calculated hierarchy using formulas
- ALLOCATION: Allocation/distribution hierarchy
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class HierarchyType(str, Enum):
    """
    Classification of hierarchy types.

    Each type has specific behaviors, validation rules, and generation patterns.
    """

    STANDARD = "standard"
    """
    Standard hierarchy for basic grouping and categorization.
    - Simple parent-child relationships
    - No special aggregation logic
    - Used for: GL accounts, departments, locations
    """

    GROUPING = "grouping"
    """
    Grouping hierarchy for aggregation and rollups.
    - Defines how values aggregate up the tree
    - Supports multiple aggregation methods (SUM, AVG, MAX, MIN)
    - Used for: Financial consolidation, reporting rollups
    """

    XREF = "xref"
    """
    Cross-reference hierarchy for dimensional linking.
    - Maps values between different dimensions
    - Supports one-to-many and many-to-many relationships
    - Used for: Account mapping, entity mapping, chart conversion
    """

    CALCULATION = "calculation"
    """
    Calculated hierarchy using formulas.
    - Derived values from other hierarchies
    - Supports SUM, SUBTRACT, MULTIPLY, DIVIDE operations
    - Used for: Calculated KPIs, ratios, margins
    """

    ALLOCATION = "allocation"
    """
    Allocation hierarchy for distribution logic.
    - Distributes values based on allocation factors
    - Supports percentage and driver-based allocation
    - Used for: Cost allocation, shared services distribution
    """


class AggregationMethod(str, Enum):
    """Methods for aggregating values up a hierarchy."""

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    WEIGHTED_AVG = "weighted_avg"


class TransformationType(str, Enum):
    """Types of transformations for hierarchy values."""

    PASSTHROUGH = "passthrough"  # No transformation
    NEGATE = "negate"  # Multiply by -1
    ABSOLUTE = "absolute"  # Take absolute value
    PERCENTAGE = "percentage"  # Convert to percentage
    SCALE = "scale"  # Multiply by a factor
    REMAP = "remap"  # Remap to different value


@dataclass
class TypeValidationRule:
    """Validation rule for a hierarchy type."""

    name: str
    description: str
    is_required: bool = False
    applies_to_types: List[HierarchyType] = field(default_factory=list)

    def validate(self, hierarchy: Any) -> tuple[bool, str]:
        """
        Validate a hierarchy against this rule.

        Args:
            hierarchy: Hierarchy object to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        raise NotImplementedError("Subclasses must implement validate()")


class RequireSourceMappingsRule(TypeValidationRule):
    """Rule that requires leaf nodes to have source mappings."""

    def __init__(self):
        super().__init__(
            name="require_source_mappings",
            description="Leaf nodes must have at least one source mapping",
            is_required=True,
            applies_to_types=[HierarchyType.STANDARD, HierarchyType.GROUPING],
        )

    def validate(self, hierarchy: Any) -> tuple[bool, str]:
        if hierarchy.is_leaf_node:
            mappings = hierarchy.source_mappings or []
            if not mappings:
                return False, f"Leaf node '{hierarchy.hierarchy_name}' has no source mappings"
        return True, ""


class RequireFormulaRule(TypeValidationRule):
    """Rule that requires calculation hierarchies to have formulas."""

    def __init__(self):
        super().__init__(
            name="require_formula",
            description="Calculation hierarchies must have a formula configuration",
            is_required=True,
            applies_to_types=[HierarchyType.CALCULATION],
        )

    def validate(self, hierarchy: Any) -> tuple[bool, str]:
        if hierarchy.hierarchy_type == HierarchyType.CALCULATION.value:
            if not hierarchy.formula_config:
                return False, f"Calculation hierarchy '{hierarchy.hierarchy_name}' has no formula"
        return True, ""


class RequireXRefTargetRule(TypeValidationRule):
    """Rule that requires XREF hierarchies to have target mappings."""

    def __init__(self):
        super().__init__(
            name="require_xref_target",
            description="XREF hierarchies must have target dimension mappings",
            is_required=True,
            applies_to_types=[HierarchyType.XREF],
        )

    def validate(self, hierarchy: Any) -> tuple[bool, str]:
        if hierarchy.hierarchy_type == HierarchyType.XREF.value:
            metadata = hierarchy.metadata_config or {}
            if not metadata.get("target_dimension"):
                return False, f"XREF hierarchy '{hierarchy.hierarchy_name}' has no target dimension"
        return True, ""


@dataclass
class HierarchyTypeConfig:
    """Configuration for a specific hierarchy type."""

    hierarchy_type: HierarchyType
    display_name: str
    description: str

    # Behavior flags
    supports_aggregation: bool = True
    supports_formulas: bool = False
    supports_source_mappings: bool = True
    supports_allocation: bool = False
    requires_parent: bool = False

    # Default settings
    default_aggregation: AggregationMethod = AggregationMethod.SUM
    default_transformation: TransformationType = TransformationType.PASSTHROUGH

    # Generation settings
    generate_unnest_view: bool = True
    generate_dimension_table: bool = True
    generate_aggregation_table: bool = False

    # Validation rules
    validation_rules: List[TypeValidationRule] = field(default_factory=list)

    # Allowed flags
    allowed_flags: Set[str] = field(default_factory=lambda: {
        "include_flag", "exclude_flag", "transform_flag",
        "calculation_flag", "active_flag", "is_leaf_node"
    })


# Type configurations
TYPE_CONFIGS: Dict[HierarchyType, HierarchyTypeConfig] = {
    HierarchyType.STANDARD: HierarchyTypeConfig(
        hierarchy_type=HierarchyType.STANDARD,
        display_name="Standard",
        description="Basic hierarchical grouping and categorization",
        supports_aggregation=True,
        supports_formulas=False,
        supports_source_mappings=True,
        default_aggregation=AggregationMethod.SUM,
        generate_unnest_view=True,
        generate_dimension_table=True,
        validation_rules=[RequireSourceMappingsRule()],
    ),
    HierarchyType.GROUPING: HierarchyTypeConfig(
        hierarchy_type=HierarchyType.GROUPING,
        display_name="Grouping",
        description="Aggregation/rollup hierarchy with business logic",
        supports_aggregation=True,
        supports_formulas=False,
        supports_source_mappings=True,
        default_aggregation=AggregationMethod.SUM,
        generate_unnest_view=True,
        generate_dimension_table=True,
        generate_aggregation_table=True,
        validation_rules=[RequireSourceMappingsRule()],
    ),
    HierarchyType.XREF: HierarchyTypeConfig(
        hierarchy_type=HierarchyType.XREF,
        display_name="Cross-Reference",
        description="Cross-reference hierarchy for dimensional linking",
        supports_aggregation=False,
        supports_formulas=False,
        supports_source_mappings=True,
        generate_unnest_view=True,
        generate_dimension_table=False,
        validation_rules=[RequireXRefTargetRule()],
    ),
    HierarchyType.CALCULATION: HierarchyTypeConfig(
        hierarchy_type=HierarchyType.CALCULATION,
        display_name="Calculation",
        description="Calculated hierarchy using formulas",
        supports_aggregation=False,
        supports_formulas=True,
        supports_source_mappings=False,
        generate_unnest_view=False,
        generate_dimension_table=False,
        validation_rules=[RequireFormulaRule()],
    ),
    HierarchyType.ALLOCATION: HierarchyTypeConfig(
        hierarchy_type=HierarchyType.ALLOCATION,
        display_name="Allocation",
        description="Allocation hierarchy for distribution logic",
        supports_aggregation=True,
        supports_formulas=False,
        supports_source_mappings=True,
        supports_allocation=True,
        generate_unnest_view=True,
        generate_dimension_table=True,
        generate_aggregation_table=True,
    ),
}


def get_type_config(hierarchy_type: HierarchyType) -> HierarchyTypeConfig:
    """Get configuration for a hierarchy type."""
    return TYPE_CONFIGS.get(hierarchy_type, TYPE_CONFIGS[HierarchyType.STANDARD])


def validate_hierarchy_type(hierarchy: Any) -> List[tuple[bool, str]]:
    """
    Validate a hierarchy against its type-specific rules.

    Args:
        hierarchy: Hierarchy object to validate

    Returns:
        List of (is_valid, error_message) tuples
    """
    results = []

    # Get hierarchy type
    h_type = HierarchyType(hierarchy.hierarchy_type) if hierarchy.hierarchy_type else HierarchyType.STANDARD
    config = get_type_config(h_type)

    # Run all validation rules
    for rule in config.validation_rules:
        is_valid, msg = rule.validate(hierarchy)
        results.append((is_valid, msg))

    return results


@dataclass
class TransformationConfig:
    """Configuration for a value transformation."""

    transformation_type: TransformationType
    scale_factor: float = 1.0
    remap_values: Dict[str, str] = field(default_factory=dict)
    conditions: List[Dict[str, Any]] = field(default_factory=list)

    def apply(self, value: Any) -> Any:
        """Apply the transformation to a value."""
        if self.transformation_type == TransformationType.PASSTHROUGH:
            return value
        elif self.transformation_type == TransformationType.NEGATE:
            return -value if isinstance(value, (int, float)) else value
        elif self.transformation_type == TransformationType.ABSOLUTE:
            return abs(value) if isinstance(value, (int, float)) else value
        elif self.transformation_type == TransformationType.SCALE:
            return value * self.scale_factor if isinstance(value, (int, float)) else value
        elif self.transformation_type == TransformationType.REMAP:
            return self.remap_values.get(str(value), value)
        return value


@dataclass
class AggregationConfig:
    """Configuration for value aggregation."""

    method: AggregationMethod
    weight_column: Optional[str] = None  # For weighted average
    filters: List[Dict[str, Any]] = field(default_factory=list)

    def get_sql_function(self) -> str:
        """Get the SQL function for this aggregation."""
        mapping = {
            AggregationMethod.SUM: "SUM",
            AggregationMethod.AVG: "AVG",
            AggregationMethod.MIN: "MIN",
            AggregationMethod.MAX: "MAX",
            AggregationMethod.COUNT: "COUNT",
            AggregationMethod.FIRST: "FIRST_VALUE",
            AggregationMethod.LAST: "LAST_VALUE",
            AggregationMethod.WEIGHTED_AVG: "SUM",  # Special handling needed
        }
        return mapping.get(self.method, "SUM")


def get_all_hierarchy_types() -> List[Dict[str, Any]]:
    """Get all hierarchy types with their configurations."""
    return [
        {
            "type": ht.value,
            "display_name": config.display_name,
            "description": config.description,
            "supports_aggregation": config.supports_aggregation,
            "supports_formulas": config.supports_formulas,
            "supports_source_mappings": config.supports_source_mappings,
            "supports_allocation": config.supports_allocation,
        }
        for ht, config in TYPE_CONFIGS.items()
    ]
