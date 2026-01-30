"""
Dimension Mapper for DataBridge Analytics V4.

Maps V3 Hierarchy structures to V4 dimension attributes for analytics.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Callable
from enum import Enum
import logging

from .v3_client import V3Hierarchy, V3Mapping


logger = logging.getLogger(__name__)


class DimensionType(str, Enum):
    """Type of dimension."""
    ACCOUNT = "account"  # P&L, Balance Sheet accounts
    ENTITY = "entity"  # Legal entities, companies
    GEOGRAPHY = "geography"  # Regions, countries, locations
    TIME = "time"  # Periods, years, quarters
    PRODUCT = "product"  # Products, services, SKUs
    CUSTOMER = "customer"  # Customers, segments
    PROJECT = "project"  # Projects, cost centers
    CUSTOM = "custom"  # Custom dimensions


@dataclass
class DimensionAttribute:
    """A dimension attribute (column)."""

    name: str
    data_type: str = "string"
    source_level: Optional[str] = None
    source_column: Optional[str] = None
    is_key: bool = False
    is_display: bool = False
    sort_order: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "source_level": self.source_level,
            "source_column": self.source_column,
            "is_key": self.is_key,
            "is_display": self.is_display,
            "sort_order": self.sort_order,
        }


@dataclass
class DimensionMember:
    """A dimension member (value)."""

    key: str
    name: str
    parent_key: Optional[str] = None
    level: int = 0
    attributes: Dict[str, Any] = field(default_factory=dict)
    sort_order: int = 0
    is_leaf: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "name": self.name,
            "parent_key": self.parent_key,
            "level": self.level,
            "attributes": self.attributes,
            "sort_order": self.sort_order,
            "is_leaf": self.is_leaf,
        }


@dataclass
class Dimension:
    """A complete dimension structure."""

    name: str
    dimension_type: DimensionType
    description: str = ""
    attributes: List[DimensionAttribute] = field(default_factory=list)
    members: List[DimensionMember] = field(default_factory=list)
    hierarchy_id: str = ""
    project_id: str = ""
    source_mappings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "dimension_type": self.dimension_type.value,
            "description": self.description,
            "attributes": [a.to_dict() for a in self.attributes],
            "members": [m.to_dict() for m in self.members],
            "hierarchy_id": self.hierarchy_id,
            "project_id": self.project_id,
            "source_mappings": self.source_mappings,
            "member_count": len(self.members),
            "attribute_count": len(self.attributes),
        }

    def get_leaf_members(self) -> List[DimensionMember]:
        """Get leaf members only."""
        return [m for m in self.members if m.is_leaf]

    def get_members_at_level(self, level: int) -> List[DimensionMember]:
        """Get members at a specific level."""
        return [m for m in self.members if m.level == level]


@dataclass
class DimensionMapperResult:
    """Result from dimension mapping operations."""

    success: bool
    message: str = ""
    dimension: Optional[Dimension] = None
    dimensions: List[Dimension] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "dimension": self.dimension.to_dict() if self.dimension else None,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "errors": self.errors,
        }


class DimensionMapper:
    """
    Maps V3 hierarchies to V4 dimension structures.

    Provides:
    - Hierarchy to dimension conversion
    - Level to attribute mapping
    - Source mapping preservation
    - Dimension type inference
    """

    # Keywords for dimension type inference
    DIMENSION_TYPE_KEYWORDS = {
        DimensionType.ACCOUNT: ["account", "gl", "p&l", "pl", "profit", "loss", "balance", "revenue", "expense", "income", "cost"],
        DimensionType.ENTITY: ["entity", "company", "legal", "subsidiary", "division", "business unit", "bu"],
        DimensionType.GEOGRAPHY: ["geography", "region", "country", "state", "city", "location", "territory"],
        DimensionType.TIME: ["time", "period", "year", "quarter", "month", "date", "fiscal"],
        DimensionType.PRODUCT: ["product", "item", "sku", "service", "category", "brand"],
        DimensionType.CUSTOMER: ["customer", "client", "segment", "channel", "market"],
        DimensionType.PROJECT: ["project", "job", "cost center", "department", "work order"],
    }

    def __init__(
        self,
        default_dimension_type: DimensionType = DimensionType.CUSTOM,
        level_name_format: str = "Level {n}",
        infer_types: bool = True,
    ):
        """
        Initialize the dimension mapper.

        Args:
            default_dimension_type: Default type for unrecognized dimensions.
            level_name_format: Format string for level attribute names.
            infer_types: Whether to infer dimension types from names.
        """
        self.default_dimension_type = default_dimension_type
        self.level_name_format = level_name_format
        self.infer_types = infer_types

    def map_hierarchy(
        self,
        hierarchy: V3Hierarchy,
        mappings: Optional[List[V3Mapping]] = None,
        dimension_type: Optional[DimensionType] = None,
    ) -> DimensionMapperResult:
        """
        Map a V3 hierarchy to a V4 dimension.

        Args:
            hierarchy: V3 hierarchy to map.
            mappings: Optional source mappings.
            dimension_type: Optional explicit dimension type.

        Returns:
            DimensionMapperResult with the mapped dimension.
        """
        try:
            # Determine dimension type
            if dimension_type:
                dim_type = dimension_type
            elif self.infer_types:
                dim_type = self._infer_dimension_type(hierarchy.hierarchy_name)
            else:
                dim_type = self.default_dimension_type

            # Create attributes from levels
            attributes = self._create_level_attributes(hierarchy)

            # Create a single member from this hierarchy
            member = DimensionMember(
                key=hierarchy.hierarchy_id,
                name=hierarchy.hierarchy_name,
                parent_key=hierarchy.parent_id,
                level=hierarchy.get_depth(),
                attributes=hierarchy.levels,
                sort_order=hierarchy.sort_order,
                is_leaf=True,  # Will be updated when processing tree
            )

            # Convert mappings
            source_mappings = []
            if mappings:
                source_mappings = [m.to_dict() for m in mappings]

            dimension = Dimension(
                name=hierarchy.hierarchy_name,
                dimension_type=dim_type,
                description=hierarchy.description,
                attributes=attributes,
                members=[member],
                hierarchy_id=hierarchy.hierarchy_id,
                project_id=hierarchy.project_id,
                source_mappings=source_mappings,
            )

            return DimensionMapperResult(
                success=True,
                message=f"Mapped hierarchy '{hierarchy.hierarchy_name}' to dimension",
                dimension=dimension,
            )

        except Exception as e:
            logger.error(f"Failed to map hierarchy: {e}")
            return DimensionMapperResult(
                success=False,
                message=f"Failed to map hierarchy: {str(e)}",
                errors=[str(e)],
            )

    def map_hierarchy_tree(
        self,
        hierarchies: List[V3Hierarchy],
        root_name: str,
        dimension_type: Optional[DimensionType] = None,
        mappings_by_hierarchy: Optional[Dict[str, List[V3Mapping]]] = None,
    ) -> DimensionMapperResult:
        """
        Map a hierarchy tree to a dimension with members.

        Args:
            hierarchies: List of V3 hierarchies forming the tree.
            root_name: Name for the dimension (typically root hierarchy name).
            dimension_type: Optional explicit dimension type.
            mappings_by_hierarchy: Mappings keyed by hierarchy_id.

        Returns:
            DimensionMapperResult with the complete dimension.
        """
        try:
            if not hierarchies:
                return DimensionMapperResult(
                    success=False,
                    errors=["No hierarchies provided"],
                )

            # Determine dimension type
            if dimension_type:
                dim_type = dimension_type
            elif self.infer_types:
                dim_type = self._infer_dimension_type(root_name)
            else:
                dim_type = self.default_dimension_type

            # Build parent-child relationships
            child_ids = {h.parent_id for h in hierarchies if h.parent_id}
            parent_ids = {h.hierarchy_id for h in hierarchies}

            # Collect all level attributes
            all_attributes = set()
            for h in hierarchies:
                for level_key in h.levels.keys():
                    all_attributes.add(level_key)

            attributes = []
            for i, level_key in enumerate(sorted(all_attributes)):
                attributes.append(DimensionAttribute(
                    name=self.level_name_format.format(n=i + 1),
                    source_level=level_key,
                    sort_order=i,
                ))

            # Create members
            members = []
            for h in hierarchies:
                is_leaf = h.hierarchy_id not in child_ids
                member = DimensionMember(
                    key=h.hierarchy_id,
                    name=h.hierarchy_name,
                    parent_key=h.parent_id,
                    level=h.get_depth(),
                    attributes=h.levels,
                    sort_order=h.sort_order,
                    is_leaf=is_leaf,
                )
                members.append(member)

            # Collect all source mappings
            source_mappings = []
            if mappings_by_hierarchy:
                for hierarchy_id, mappings in mappings_by_hierarchy.items():
                    for m in mappings:
                        mapping_dict = m.to_dict()
                        source_mappings.append(mapping_dict)

            # Find root hierarchy for project_id
            root_hierarchy = hierarchies[0]
            for h in hierarchies:
                if h.parent_id is None:
                    root_hierarchy = h
                    break

            dimension = Dimension(
                name=root_name,
                dimension_type=dim_type,
                description=f"Dimension from V3 hierarchy tree: {root_name}",
                attributes=attributes,
                members=members,
                hierarchy_id=root_hierarchy.hierarchy_id,
                project_id=root_hierarchy.project_id,
                source_mappings=source_mappings,
            )

            return DimensionMapperResult(
                success=True,
                message=f"Mapped {len(hierarchies)} hierarchies to dimension '{root_name}' with {len(members)} members",
                dimension=dimension,
            )

        except Exception as e:
            logger.error(f"Failed to map hierarchy tree: {e}")
            return DimensionMapperResult(
                success=False,
                message=f"Failed to map hierarchy tree: {str(e)}",
                errors=[str(e)],
            )

    def map_hierarchies_to_dimensions(
        self,
        hierarchies: List[V3Hierarchy],
        group_by: str = "project_id",
    ) -> DimensionMapperResult:
        """
        Map multiple hierarchies to multiple dimensions.

        Args:
            hierarchies: List of V3 hierarchies.
            group_by: How to group hierarchies into dimensions.

        Returns:
            DimensionMapperResult with list of dimensions.
        """
        try:
            # Group hierarchies
            groups: Dict[str, List[V3Hierarchy]] = {}
            for h in hierarchies:
                if group_by == "project_id":
                    key = h.project_id
                elif group_by == "formula_group":
                    key = h.formula_group or "default"
                else:
                    key = "all"

                if key not in groups:
                    groups[key] = []
                groups[key].append(h)

            # Map each group to a dimension
            dimensions = []
            for group_key, group_hierarchies in groups.items():
                # Use the first hierarchy's name as dimension name
                root_name = group_hierarchies[0].hierarchy_name if group_hierarchies else group_key

                result = self.map_hierarchy_tree(
                    hierarchies=group_hierarchies,
                    root_name=root_name,
                )

                if result.success and result.dimension:
                    dimensions.append(result.dimension)

            return DimensionMapperResult(
                success=True,
                message=f"Mapped {len(hierarchies)} hierarchies to {len(dimensions)} dimensions",
                dimensions=dimensions,
            )

        except Exception as e:
            logger.error(f"Failed to map hierarchies: {e}")
            return DimensionMapperResult(
                success=False,
                message=f"Failed to map hierarchies: {str(e)}",
                errors=[str(e)],
            )

    def generate_dimension_sql(
        self,
        dimension: Dimension,
        table_name: Optional[str] = None,
        dialect: str = "postgresql",
    ) -> str:
        """
        Generate SQL to create a dimension table.

        Args:
            dimension: Dimension to generate SQL for.
            table_name: Optional table name (defaults to dimension name).
            dialect: SQL dialect.

        Returns:
            CREATE TABLE SQL statement.
        """
        table = table_name or f"dim_{dimension.name.lower().replace(' ', '_')}"

        columns = [
            f"  {dimension.name.lower().replace(' ', '_')}_key VARCHAR(100) PRIMARY KEY",
            f"  {dimension.name.lower().replace(' ', '_')}_name VARCHAR(255) NOT NULL",
            "  parent_key VARCHAR(100)",
            "  level_depth INTEGER",
            "  is_leaf BOOLEAN DEFAULT TRUE",
            "  sort_order INTEGER DEFAULT 0",
        ]

        # Add level columns
        for attr in dimension.attributes:
            col_name = attr.name.lower().replace(' ', '_')
            columns.append(f"  {col_name} VARCHAR(255)")

        sql = f"CREATE TABLE {table} (\n"
        sql += ",\n".join(columns)
        sql += "\n);"

        return sql

    def _infer_dimension_type(self, name: str) -> DimensionType:
        """Infer dimension type from name."""
        name_lower = name.lower()

        for dim_type, keywords in self.DIMENSION_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword in name_lower:
                    return dim_type

        return self.default_dimension_type

    def _create_level_attributes(self, hierarchy: V3Hierarchy) -> List[DimensionAttribute]:
        """Create dimension attributes from hierarchy levels."""
        attributes = []

        for i in range(1, 11):
            level_key = f"level_{i}"
            if level_key in hierarchy.levels and hierarchy.levels[level_key]:
                attributes.append(DimensionAttribute(
                    name=self.level_name_format.format(n=i),
                    source_level=level_key,
                    sort_order=i - 1,
                ))

        return attributes
