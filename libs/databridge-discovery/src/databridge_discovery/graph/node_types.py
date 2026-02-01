"""
Node and edge types for the semantic graph.

This module defines the Pydantic models for nodes (tables, columns, hierarchies)
and edges (relationships) in the semantic graph.
"""

from __future__ import annotations

from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class NodeType(str, Enum):
    """Types of nodes in the semantic graph."""
    TABLE = "table"
    COLUMN = "column"
    HIERARCHY = "hierarchy"
    HIERARCHY_LEVEL = "hierarchy_level"
    HIERARCHY_NODE = "hierarchy_node"
    SCHEMA = "schema"
    DATABASE = "database"
    CONCEPT = "concept"  # Abstract concept grouping related elements
    FORMULA = "formula"


class EdgeType(str, Enum):
    """Types of edges in the semantic graph."""
    # Table relationships
    FOREIGN_KEY = "foreign_key"
    JOIN = "join"
    REFERENCES = "references"

    # Column relationships
    COLUMN_OF = "column_of"  # Column belongs to table
    DERIVED_FROM = "derived_from"  # Column derived from another
    MAPS_TO = "maps_to"  # Column maps to hierarchy

    # Hierarchy relationships
    PARENT_OF = "parent_of"  # Hierarchy parent-child
    CHILD_OF = "child_of"
    LEVEL_OF = "level_of"  # Level belongs to hierarchy
    MEMBER_OF = "member_of"  # Node is member of level

    # Schema relationships
    IN_SCHEMA = "in_schema"
    IN_DATABASE = "in_database"

    # Semantic relationships
    SIMILAR_TO = "similar_to"  # Semantically similar
    SAME_AS = "same_as"  # Represents same entity
    RELATED_TO = "related_to"  # Generally related

    # Formula relationships
    FORMULA_INPUT = "formula_input"
    FORMULA_OUTPUT = "formula_output"


class GraphNode(BaseModel):
    """Base class for all graph nodes."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique node ID")
    node_type: NodeType = Field(..., description="Type of node")
    name: str = Field(..., description="Node name")
    display_name: str | None = Field(None, description="Human-readable display name")
    description: str | None = Field(None, description="Node description")

    # Metadata
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    tags: list[str] = Field(default_factory=list, description="Tags for categorization")

    # Embedding
    embedding: list[float] | None = Field(None, description="Vector embedding")
    embedding_model: str | None = Field(None, description="Model used for embedding")

    # Source tracking
    source_file: str | None = Field(None, description="Source file")
    source_query: str | None = Field(None, description="Source SQL query")

    model_config = {"extra": "allow"}

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GraphNode):
            return self.id == other.id
        return False


class TableNode(GraphNode):
    """Node representing a database table."""

    node_type: NodeType = Field(default=NodeType.TABLE)

    # Table metadata
    database: str | None = Field(None, description="Database name")
    schema_name: str | None = Field(None, description="Schema name")
    table_name: str = Field(..., description="Table name")

    # Structure
    column_count: int = Field(default=0, description="Number of columns")
    row_count: int | None = Field(None, description="Estimated row count")
    primary_key: list[str] = Field(default_factory=list, description="Primary key columns")

    # Classification
    table_type: str = Field(default="unknown", description="Table type: fact, dimension, etc.")
    is_view: bool = Field(default=False, description="True if this is a view")

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        parts = [p for p in [self.database, self.schema_name, self.table_name] if p]
        return ".".join(parts)

    @classmethod
    def from_parsed_table(cls, parsed_table: Any) -> "TableNode":
        """Create TableNode from a ParsedTable."""
        return cls(
            name=parsed_table.full_name,
            table_name=parsed_table.name,
            database=parsed_table.database,
            schema_name=parsed_table.schema_name,
            display_name=parsed_table.alias or parsed_table.name,
            is_view=parsed_table.is_subquery,
        )


class ColumnNode(GraphNode):
    """Node representing a database column."""

    node_type: NodeType = Field(default=NodeType.COLUMN)

    # Column metadata
    table_id: str | None = Field(None, description="ID of parent table node")
    column_name: str = Field(..., description="Column name")
    data_type: str = Field(default="unknown", description="Column data type")

    # Classification
    column_type: str = Field(default="unknown", description="Column type: measure, dimension, key, etc.")
    is_nullable: bool = Field(default=True, description="Whether column allows nulls")
    is_primary_key: bool = Field(default=False, description="Is part of primary key")
    is_foreign_key: bool = Field(default=False, description="Is a foreign key")

    # Statistics (optional)
    distinct_count: int | None = Field(None, description="Number of distinct values")
    null_count: int | None = Field(None, description="Number of null values")
    sample_values: list[str] = Field(default_factory=list, description="Sample values")

    @property
    def qualified_name(self) -> str:
        """Get table.column name."""
        if self.table_id:
            return f"{self.table_id}.{self.column_name}"
        return self.column_name

    @classmethod
    def from_parsed_column(cls, parsed_column: Any, table_id: str | None = None) -> "ColumnNode":
        """Create ColumnNode from a ParsedColumn."""
        return cls(
            name=parsed_column.name,
            column_name=parsed_column.source_name or parsed_column.name,
            table_id=table_id,
            data_type=parsed_column.data_type.value if hasattr(parsed_column.data_type, 'value') else str(parsed_column.data_type),
            column_type="measure" if parsed_column.aggregation else "dimension",
        )


class HierarchyNode(GraphNode):
    """Node representing a hierarchy or hierarchy element."""

    node_type: NodeType = Field(default=NodeType.HIERARCHY)

    # Hierarchy metadata
    hierarchy_id: str | None = Field(None, description="Librarian hierarchy ID")
    level_number: int | None = Field(None, description="Level number in hierarchy")
    parent_id: str | None = Field(None, description="Parent node ID")

    # Value
    value: str | None = Field(None, description="Hierarchy node value")
    sort_order: int = Field(default=0, description="Sort order")

    # Mapping
    source_column: str | None = Field(None, description="Source column for mapping")
    source_values: list[str] = Field(default_factory=list, description="Source values that map here")

    # Aggregation
    formula_type: str | None = Field(None, description="Formula type: SUM, SUBTRACT, etc.")
    is_calculated: bool = Field(default=False, description="Is a calculated node")

    @classmethod
    def from_extracted_hierarchy(cls, hierarchy: Any) -> "HierarchyNode":
        """Create HierarchyNode from ExtractedHierarchy."""
        return cls(
            id=hierarchy.id,
            name=hierarchy.name,
            hierarchy_id=hierarchy.id,
            source_column=hierarchy.source_column,
            description=f"Extracted from {hierarchy.source_column}",
            metadata={
                "entity_type": hierarchy.entity_type.value if hasattr(hierarchy.entity_type, 'value') else str(hierarchy.entity_type),
                "confidence": hierarchy.confidence_score,
                "total_levels": hierarchy.total_levels,
                "total_nodes": hierarchy.total_nodes,
            },
        )


class SchemaNode(GraphNode):
    """Node representing a database schema."""

    node_type: NodeType = Field(default=NodeType.SCHEMA)

    database: str | None = Field(None, description="Database name")
    schema_name: str = Field(..., description="Schema name")
    table_count: int = Field(default=0, description="Number of tables")


class ConceptNode(GraphNode):
    """Node representing an abstract concept grouping related elements."""

    node_type: NodeType = Field(default=NodeType.CONCEPT)

    # Concept metadata
    concept_type: str = Field(default="generic", description="Type: entity, dimension, metric, etc.")
    member_ids: list[str] = Field(default_factory=list, description="IDs of member nodes")
    confidence: float = Field(default=0.0, description="Confidence in concept grouping")

    # Canonical form
    canonical_name: str | None = Field(None, description="Canonical/standardized name")
    aliases: list[str] = Field(default_factory=list, description="Known aliases")


class GraphEdge(BaseModel):
    """Edge connecting two nodes in the semantic graph."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique edge ID")
    edge_type: EdgeType = Field(..., description="Type of edge")
    source_id: str = Field(..., description="Source node ID")
    target_id: str = Field(..., description="Target node ID")

    # Edge metadata
    weight: float = Field(default=1.0, description="Edge weight/strength")
    confidence: float = Field(default=1.0, description="Confidence in relationship")
    label: str | None = Field(None, description="Edge label")

    # For join relationships
    join_condition: str | None = Field(None, description="SQL join condition")
    join_type: str | None = Field(None, description="Join type: INNER, LEFT, etc.")

    # Metadata
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"extra": "allow"}

    def __hash__(self) -> int:
        return hash((self.source_id, self.target_id, self.edge_type))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, GraphEdge):
            return (
                self.source_id == other.source_id
                and self.target_id == other.target_id
                and self.edge_type == other.edge_type
            )
        return False

    @property
    def key(self) -> tuple[str, str, str]:
        """Get edge key as (source, target, type) tuple."""
        return (self.source_id, self.target_id, self.edge_type.value)


class GraphStats(BaseModel):
    """Statistics about a semantic graph."""

    node_count: int = Field(default=0, description="Total nodes")
    edge_count: int = Field(default=0, description="Total edges")

    # Node counts by type
    table_count: int = Field(default=0, description="Table nodes")
    column_count: int = Field(default=0, description="Column nodes")
    hierarchy_count: int = Field(default=0, description="Hierarchy nodes")
    concept_count: int = Field(default=0, description="Concept nodes")

    # Edge counts by type
    join_count: int = Field(default=0, description="Join edges")
    foreign_key_count: int = Field(default=0, description="Foreign key edges")
    similarity_count: int = Field(default=0, description="Similarity edges")

    # Graph metrics
    density: float = Field(default=0.0, description="Graph density")
    avg_degree: float = Field(default=0.0, description="Average node degree")
    connected_components: int = Field(default=0, description="Number of connected components")
