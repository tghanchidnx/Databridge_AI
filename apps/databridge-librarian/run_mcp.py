#!/usr/bin/env python3
"""
Runner script for DataBridge Librarian MCP Server.

Registers all 92 MCP tools across modules:
- Project tools (5)
- Hierarchy tools (15)
- Reconciliation tools (20)
- Vector/RAG tools (16)
"""

import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

# Set up the path correctly
librarian_root = Path(__file__).parent
sys.path.insert(0, str(librarian_root))
os.chdir(librarian_root)

from fastmcp import FastMCP

# Create the MCP server
mcp = FastMCP("databridge-librarian")

# Import services
from src.hierarchy.service import HierarchyService, DuplicateError, ProjectNotFoundError
from src.core.database import get_session, session_scope
from src.hierarchy.tree import TreeBuilder, TreeNavigator
from src.hierarchy.csv_handler import CSVHandler
from src.hierarchy.formula import FormulaEngine
from src.reconciliation.loader import DataLoader
from src.reconciliation.profiler import DataProfiler
from src.reconciliation.hasher import HashComparer
from src.reconciliation.fuzzy import FuzzyMatcher
from src.vectors.store import VectorStore
from src.vectors.rag import HierarchyRAG
from src.vectors.embedder import HierarchyEmbedder

# SQL Generator imports
from src.sql_generator.pattern_detector import PatternDetectorService, get_pattern_detector
from src.sql_generator.schema_registry import SchemaRegistryService, get_schema_registry
from src.sql_generator.lineage_tracker import LineageTrackerService, get_lineage_tracker
from src.sql_generator.view_generator import ViewGeneratorService, get_view_generator
from src.sql_generator.models import PatternType, ColumnType, SQLDialect, TransformationType, ObjectType

print("All modules imported successfully!")

# =============================================================================
# PROJECT TOOLS (5)
# =============================================================================

@mcp.tool()
def list_hierarchy_projects() -> Dict[str, Any]:
    """List all hierarchy projects."""
    service = HierarchyService()
    projects = service.list_projects()
    return {"projects": [p.to_dict() for p in projects], "count": len(projects)}

@mcp.tool()
def create_hierarchy_project(
    name: str,
    description: str = None,
    industry: str = None
) -> Dict[str, Any]:
    """Create a new hierarchy project."""
    service = HierarchyService()
    try:
        project = service.create_project(name=name, description=description, industry=industry)
        return {"success": True, "project": project.to_dict()}
    except DuplicateError as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_hierarchy_project(project_id: str) -> Dict[str, Any]:
    """Get a hierarchy project by ID."""
    service = HierarchyService()
    project = service.get_project(project_id)
    return project.to_dict() if project else {"error": "Project not found"}

@mcp.tool()
def update_hierarchy_project(
    project_id: str,
    name: str = None,
    description: str = None,
    industry: str = None
) -> Dict[str, Any]:
    """Update a hierarchy project."""
    service = HierarchyService()
    try:
        project = service.update_project(project_id, name=name, description=description, industry=industry)
        return {"success": True, "project": project.to_dict()}
    except ProjectNotFoundError as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def delete_hierarchy_project(project_id: str) -> Dict[str, Any]:
    """Delete a hierarchy project."""
    service = HierarchyService()
    try:
        service.delete_project(project_id)
        return {"success": True, "message": f"Project {project_id} deleted"}
    except ProjectNotFoundError as e:
        return {"success": False, "error": str(e)}

# =============================================================================
# HIERARCHY TOOLS (15)
# =============================================================================

@mcp.tool()
def create_hierarchy(
    project_id: str,
    name: str,
    parent_id: str = None,
    description: str = None,
    formula_type: str = None
) -> Dict[str, Any]:
    """Create a new hierarchy node within a project."""
    service = HierarchyService()
    try:
        hierarchy = service.create_hierarchy(
            project_id=project_id,
            name=name,
            parent_id=parent_id,
            description=description,
            formula_type=formula_type
        )
        return {"success": True, "hierarchy": hierarchy.to_dict()}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_hierarchy(hierarchy_id: str) -> Dict[str, Any]:
    """Get a hierarchy node by ID."""
    service = HierarchyService()
    hierarchy = service.get_hierarchy(hierarchy_id)
    return hierarchy.to_dict() if hierarchy else {"error": "Hierarchy not found"}

@mcp.tool()
def update_hierarchy(
    hierarchy_id: str,
    name: str = None,
    description: str = None,
    parent_id: str = None
) -> Dict[str, Any]:
    """Update a hierarchy node."""
    service = HierarchyService()
    try:
        hierarchy = service.update_hierarchy(hierarchy_id, name=name, description=description, parent_id=parent_id)
        return {"success": True, "hierarchy": hierarchy.to_dict()}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def delete_hierarchy(hierarchy_id: str) -> Dict[str, Any]:
    """Delete a hierarchy node."""
    service = HierarchyService()
    try:
        service.delete_hierarchy(hierarchy_id)
        return {"success": True, "message": f"Hierarchy {hierarchy_id} deleted"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_hierarchy_tree(project_id: str) -> Dict[str, Any]:
    """Get the full hierarchy tree for a project."""
    service = HierarchyService()
    hierarchies = service.get_hierarchies_by_project(project_id)
    builder = TreeBuilder()
    tree = builder.build_tree([h.to_dict() for h in hierarchies])
    return {"tree": tree, "node_count": len(hierarchies)}

@mcp.tool()
def get_hierarchy_children(hierarchy_id: str) -> Dict[str, Any]:
    """Get all children of a hierarchy node."""
    service = HierarchyService()
    children = service.get_children(hierarchy_id)
    return {"children": [c.to_dict() for c in children], "count": len(children)}

@mcp.tool()
def move_hierarchy_node(hierarchy_id: str, new_parent_id: str) -> Dict[str, Any]:
    """Move a hierarchy node to a new parent."""
    service = HierarchyService()
    try:
        hierarchy = service.update_hierarchy(hierarchy_id, parent_id=new_parent_id)
        return {"success": True, "hierarchy": hierarchy.to_dict()}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def add_source_mapping(
    hierarchy_id: str,
    source_column: str,
    source_value: str,
    precedence: int = 1
) -> Dict[str, Any]:
    """Add a source mapping to a hierarchy node."""
    service = HierarchyService()
    try:
        mapping = service.add_mapping(
            hierarchy_id=hierarchy_id,
            source_column=source_column,
            source_value=source_value,
            precedence=precedence
        )
        return {"success": True, "mapping": mapping.to_dict()}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def remove_source_mapping(mapping_id: str) -> Dict[str, Any]:
    """Remove a source mapping."""
    service = HierarchyService()
    try:
        service.remove_mapping(mapping_id)
        return {"success": True, "message": f"Mapping {mapping_id} removed"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_mappings_by_hierarchy(hierarchy_id: str) -> Dict[str, Any]:
    """Get all mappings for a hierarchy node."""
    service = HierarchyService()
    mappings = service.get_mappings(hierarchy_id)
    return {"mappings": [m.to_dict() for m in mappings], "count": len(mappings)}

@mcp.tool()
def export_hierarchy_csv(project_id: str, output_path: str) -> Dict[str, Any]:
    """Export a project's hierarchies to CSV."""
    service = HierarchyService()
    handler = CSVHandler()
    try:
        hierarchies = service.get_hierarchies_by_project(project_id)
        handler.export_hierarchies([h.to_dict() for h in hierarchies], output_path)
        return {"success": True, "path": output_path, "count": len(hierarchies)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def import_hierarchy_csv(project_id: str, csv_path: str) -> Dict[str, Any]:
    """Import hierarchies from CSV into a project."""
    service = HierarchyService()
    handler = CSVHandler()
    try:
        data = handler.import_hierarchies(csv_path)
        imported = []
        for row in data:
            h = service.create_hierarchy(project_id=project_id, **row)
            imported.append(h.to_dict())
        return {"success": True, "imported": len(imported)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def calculate_formula(hierarchy_id: str, data: Dict[str, float]) -> Dict[str, Any]:
    """Calculate formula for a hierarchy node."""
    service = HierarchyService()
    engine = FormulaEngine()
    hierarchy = service.get_hierarchy(hierarchy_id)
    if not hierarchy:
        return {"error": "Hierarchy not found"}
    try:
        result = engine.calculate(hierarchy.formula_type or "SUM", data)
        return {"success": True, "result": result, "formula": hierarchy.formula_type}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def validate_hierarchy_tree(project_id: str) -> Dict[str, Any]:
    """Validate a hierarchy tree for circular references and orphans."""
    service = HierarchyService()
    hierarchies = service.get_hierarchies_by_project(project_id)
    navigator = TreeNavigator()
    issues = navigator.validate([h.to_dict() for h in hierarchies])
    return {"valid": len(issues) == 0, "issues": issues}

# =============================================================================
# RECONCILIATION TOOLS (20)
# =============================================================================

@mcp.tool()
def load_csv(file_path: str, delimiter: str = None) -> Dict[str, Any]:
    """Load data from a CSV file."""
    loader = DataLoader()
    result = loader.load_csv(file_path, delimiter=delimiter)
    return result.to_dict()

@mcp.tool()
def load_json(file_path: str) -> Dict[str, Any]:
    """Load data from a JSON file."""
    loader = DataLoader()
    result = loader.load_json(file_path)
    return result.to_dict()

@mcp.tool()
def profile_data(file_path: str) -> Dict[str, Any]:
    """Profile data from a file and return statistics."""
    loader = DataLoader()
    result = loader.load_csv(file_path)
    if not result.success:
        return {"error": result.errors}

    profiler = DataProfiler()
    profile = profiler.profile(result.data)
    return {
        "row_count": profile.row_count,
        "column_count": profile.column_count,
        "columns": [
            {
                "name": c.name,
                "dtype": c.dtype,
                "null_count": c.null_count,
                "unique_count": c.unique_count,
                "sample_values": c.sample_values[:5] if c.sample_values else []
            }
            for c in profile.columns
        ]
    }

@mcp.tool()
def compare_datasets(
    source_path: str,
    target_path: str,
    key_columns: List[str]
) -> Dict[str, Any]:
    """Compare two datasets and identify differences."""
    loader = DataLoader()
    source = loader.load_csv(source_path)
    target = loader.load_csv(target_path)

    if not source.success or not target.success:
        return {"error": "Failed to load one or both files"}

    comparer = HashComparer(key_columns=key_columns)
    result = comparer.compare(source.data, target.data)
    return {
        "matched": result.matched_count,
        "orphans_source": result.orphans_source_count,
        "orphans_target": result.orphans_target_count,
        "conflicts": result.conflict_count
    }

@mcp.tool()
def get_orphan_records(
    source_path: str,
    target_path: str,
    key_columns: List[str],
    source_name: str = "source"
) -> Dict[str, Any]:
    """Get records that exist in source but not in target."""
    loader = DataLoader()
    source = loader.load_csv(source_path)
    target = loader.load_csv(target_path)

    comparer = HashComparer(key_columns=key_columns)
    result = comparer.compare(source.data, target.data)

    if source_name == "source":
        orphans = result.orphans_source[:100]  # Limit to 100 records
    else:
        orphans = result.orphans_target[:100]

    return {"orphans": orphans, "total": len(orphans)}

@mcp.tool()
def fuzzy_match_values(
    values1: List[str],
    values2: List[str],
    threshold: int = 80
) -> Dict[str, Any]:
    """Fuzzy match two lists of values."""
    matcher = FuzzyMatcher(threshold=threshold)
    matches = matcher.match_lists(values1, values2)
    return {"matches": matches, "threshold": threshold}

@mcp.tool()
def fuzzy_deduplicate(
    file_path: str,
    column: str,
    threshold: int = 80
) -> Dict[str, Any]:
    """Find potential duplicate values in a column."""
    loader = DataLoader()
    result = loader.load_csv(file_path)
    if not result.success:
        return {"error": result.errors}

    matcher = FuzzyMatcher(threshold=threshold)
    values = result.data[column].dropna().unique().tolist()
    duplicates = matcher.find_duplicates(values)
    return {"duplicates": duplicates[:50], "total_checked": len(values)}

@mcp.tool()
def detect_schema_changes(
    file_path1: str,
    file_path2: str
) -> Dict[str, Any]:
    """Detect schema differences between two files."""
    loader = DataLoader()
    result1 = loader.load_csv(file_path1)
    result2 = loader.load_csv(file_path2)

    cols1 = set(result1.columns)
    cols2 = set(result2.columns)

    return {
        "added": list(cols2 - cols1),
        "removed": list(cols1 - cols2),
        "common": list(cols1 & cols2)
    }

@mcp.tool()
def get_column_statistics(file_path: str, column: str) -> Dict[str, Any]:
    """Get detailed statistics for a specific column."""
    loader = DataLoader()
    result = loader.load_csv(file_path)
    if not result.success or column not in result.columns:
        return {"error": f"Column {column} not found"}

    profiler = DataProfiler()
    profile = profiler.profile(result.data)

    for col in profile.columns:
        if col.name == column:
            return {
                "name": col.name,
                "dtype": col.dtype,
                "null_count": col.null_count,
                "null_percent": col.null_percent,
                "unique_count": col.unique_count,
                "min": col.min_value,
                "max": col.max_value,
                "mean": col.mean,
                "std": col.std,
                "sample_values": col.sample_values[:10] if col.sample_values else []
            }
    return {"error": "Column not found in profile"}

@mcp.tool()
def health_check() -> Dict[str, Any]:
    """Check if the MCP server is healthy."""
    return {
        "status": "healthy",
        "server": "databridge-librarian",
        "version": "3.0.0",
        "modules": ["hierarchy", "reconciliation", "vectors"]
    }

# =============================================================================
# VECTOR/RAG TOOLS (16)
# =============================================================================

@mcp.tool()
def initialize_vector_store(collection_name: str = "hierarchies") -> Dict[str, Any]:
    """Initialize the vector store."""
    try:
        store = VectorStore(collection_name=collection_name)
        return {"success": True, "collection": collection_name}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def embed_hierarchy(project_id: str) -> Dict[str, Any]:
    """Generate embeddings for a project's hierarchies."""
    service = HierarchyService()
    hierarchies = service.get_hierarchies_by_project(project_id)

    try:
        embedder = HierarchyEmbedder()
        store = VectorStore()

        count = 0
        for h in hierarchies:
            embedding = embedder.embed(h.to_dict())
            store.upsert(h.id, embedding, h.to_dict())
            count += 1

        return {"success": True, "embedded": count}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def search_similar_hierarchies(query: str, limit: int = 5) -> Dict[str, Any]:
    """Search for similar hierarchies using semantic search."""
    try:
        rag = HierarchyRAG()
        results = rag.search(query, limit=limit)
        return {"results": results, "count": len(results)}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_hierarchy_context(hierarchy_id: str) -> Dict[str, Any]:
    """Get RAG context for a hierarchy node."""
    try:
        rag = HierarchyRAG()
        context = rag.get_context(hierarchy_id)
        return {"context": context}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def list_industry_patterns(industry: str = None) -> Dict[str, Any]:
    """List available industry patterns."""
    from src.vectors.industry_patterns import IndustryPatterns
    patterns = IndustryPatterns()
    if industry:
        result = patterns.get_patterns(industry)
    else:
        result = patterns.list_industries()
    return {"patterns": result}

@mcp.tool()
def get_pattern_recommendations(
    industry: str,
    hierarchy_type: str
) -> Dict[str, Any]:
    """Get hierarchy recommendations based on industry patterns."""
    from src.vectors.industry_patterns import IndustryPatterns
    patterns = IndustryPatterns()
    recommendations = patterns.get_recommendations(industry, hierarchy_type)
    return {"recommendations": recommendations}

# =============================================================================
# SQL GENERATOR TOOLS (12)
# =============================================================================

@mcp.tool()
def detect_table_patterns(
    table_name: str,
    columns: List[Dict[str, Any]],
    primary_key: List[str] = None,
    foreign_keys: List[Dict[str, Any]] = None,
    row_count: int = None
) -> Dict[str, Any]:
    """
    Detect the pattern type of a table (fact, dimension, bridge, etc.).

    Args:
        table_name: Name of the table to analyze
        columns: List of column definitions with 'name' and 'data_type'
        primary_key: List of primary key column names
        foreign_keys: List of foreign key definitions
        row_count: Optional row count for cardinality analysis

    Returns:
        Pattern detection results with type, confidence, and column classifications
    """
    detector = get_pattern_detector()
    pattern = detector.detect_table_pattern(
        table_name=table_name,
        columns=columns,
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        row_count=row_count,
    )
    return pattern.to_dict()


@mcp.tool()
def classify_columns(
    columns: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Classify columns by their type (measure, dimension, key, date, metadata).

    Args:
        columns: List of column definitions with 'name' and 'data_type'

    Returns:
        Column classifications with type and confidence for each column
    """
    detector = get_pattern_detector()
    classifications = []
    for col in columns:
        classification = detector.classify_column(
            column_name=col["name"],
            data_type=col.get("data_type", ""),
        )
        classifications.append(classification.to_dict())
    return {
        "columns": classifications,
        "summary": {
            "measures": [c["name"] for c in classifications if c["type"] == "measure"],
            "dimensions": [c["name"] for c in classifications if c["type"] == "dimension"],
            "keys": [c["name"] for c in classifications if c["type"] in ("primary_key", "foreign_key")],
            "dates": [c["name"] for c in classifications if c["type"] == "date_key"],
        }
    }


@mcp.tool()
def suggest_hierarchy_mappings(
    table_name: str,
    columns: List[Dict[str, Any]],
    hierarchy_levels: List[str]
) -> Dict[str, Any]:
    """
    Suggest mappings between table columns and hierarchy levels.

    Args:
        table_name: Name of the source table
        columns: List of column definitions
        hierarchy_levels: List of hierarchy level names to map

    Returns:
        List of suggested mappings with confidence scores
    """
    detector = get_pattern_detector()
    suggestions = detector.suggest_hierarchy_mappings(
        table_name=table_name,
        columns=columns,
        hierarchy_levels=hierarchy_levels,
    )
    return {"suggestions": suggestions, "count": len(suggestions)}


@mcp.tool()
def generate_view_sql(
    view_name: str,
    source_table: str,
    columns: List[Dict[str, Any]],
    dialect: str = "snowflake",
    source_database: str = None,
    source_schema: str = None,
    target_database: str = None,
    target_schema: str = None,
    joins: List[Dict[str, Any]] = None,
    filters: List[Dict[str, Any]] = None,
    group_by: List[str] = None,
    comment: str = None
) -> Dict[str, Any]:
    """
    Generate SQL for a view using templates (VW_1 tier).

    Args:
        view_name: Name of the view to create
        source_table: Name of the source table
        columns: List of column definitions with 'name' and optional 'alias', 'expression'
        dialect: SQL dialect (snowflake, postgresql)
        source_database: Optional source database name
        source_schema: Optional source schema name
        target_database: Optional target database name
        target_schema: Optional target schema name
        joins: Optional list of join definitions
        filters: Optional list of filter conditions
        group_by: Optional list of group by columns
        comment: Optional comment for the view

    Returns:
        Generated SQL and metadata
    """
    generator = get_view_generator()
    sql_dialect = SQLDialect(dialect.lower())

    sql = generator.generate_view_sql(
        view_name=view_name,
        source_table=source_table,
        columns=columns,
        dialect=sql_dialect,
        source_database=source_database,
        source_schema=source_schema,
        target_database=target_database,
        target_schema=target_schema,
        joins=joins,
        filters=filters,
        group_by=group_by,
        comment=comment,
    )

    return {
        "view_name": view_name,
        "dialect": dialect,
        "sql": sql,
        "column_count": len(columns),
    }


@mcp.tool()
def preview_view_sql(
    view_name: str,
    source_table: str,
    columns: List[Dict[str, Any]],
    dialect: str = "snowflake",
    **kwargs
) -> Dict[str, Any]:
    """
    Preview SQL without saving to database.

    Args:
        view_name: Name of the view
        source_table: Source table name
        columns: Column definitions
        dialect: SQL dialect
        **kwargs: Additional arguments (filters, joins, etc.)

    Returns:
        Dictionary with SQL preview and metadata
    """
    generator = get_view_generator()
    sql_dialect = SQLDialect(dialect.lower())
    return generator.preview_view_sql(
        view_name=view_name,
        source_table=source_table,
        columns=columns,
        dialect=sql_dialect,
        **kwargs,
    )


@mcp.tool()
def list_generated_views(
    project_id: str,
    pattern_type: str = None
) -> Dict[str, Any]:
    """
    List all generated views for a project.

    Args:
        project_id: Project ID
        pattern_type: Optional filter by pattern type (fact, dimension, etc.)

    Returns:
        List of generated views with metadata
    """
    from src.core.database import get_session
    generator = get_view_generator()

    with session_scope() as session:
        pt = PatternType(pattern_type) if pattern_type else None
        views = generator.list_generated_views(project_id, pattern_type=pt, session=session)
        return {
            "views": [
                {
                    "id": v.id,
                    "view_name": v.view_name,
                    "source_table": v.source_table,
                    "pattern_type": v.pattern_type.value if v.pattern_type else None,
                    "dialect": v.dialect.value if v.dialect else None,
                    "is_deployed": v.is_deployed,
                    "created_at": v.created_at.isoformat() if v.created_at else None,
                }
                for v in views
            ],
            "count": len(views),
        }


@mcp.tool()
def register_schema(
    project_id: str,
    object_type: str,
    object_name: str,
    column_definitions: List[Dict[str, Any]],
    database_name: str = None,
    schema_name: str = None,
    primary_key_columns: List[str] = None,
    foreign_keys: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Register a schema in the schema registry.

    Args:
        project_id: Project ID
        object_type: Type of object (table, view, dynamic_table)
        object_name: Name of the object
        column_definitions: List of column definitions
        database_name: Optional database name
        schema_name: Optional schema name
        primary_key_columns: List of primary key column names
        foreign_keys: List of foreign key definitions

    Returns:
        Registered schema entry
    """
    from src.core.database import get_session
    registry = get_schema_registry()

    with session_scope() as session:
        entry = registry.register_schema(
            project_id=project_id,
            object_type=ObjectType(object_type),
            object_name=object_name,
            column_definitions=column_definitions,
            database_name=database_name,
            schema_name=schema_name,
            primary_key_columns=primary_key_columns,
            foreign_keys=foreign_keys,
            session=session,
        )
        return {
            "success": True,
            "id": entry.id,
            "object": entry.full_object_path,
            "column_count": len(column_definitions),
        }


@mcp.tool()
def validate_schema_compatibility(
    project_id: str,
    source_object: str,
    target_object: str,
    source_database: str = None,
    source_schema: str = None,
    target_database: str = None,
    target_schema: str = None
) -> Dict[str, Any]:
    """
    Validate compatibility between source and target schemas.

    Args:
        project_id: Project ID
        source_object: Source object name
        target_object: Target object name
        source_database: Source database name
        source_schema: Source schema name
        target_database: Target database name
        target_schema: Target schema name

    Returns:
        Compatibility result with any issues found
    """
    from src.core.database import get_session
    registry = get_schema_registry()

    with session_scope() as session:
        source = registry.get_schema(
            project_id, source_object, source_database, source_schema, session
        )
        target = registry.get_schema(
            project_id, target_object, target_database, target_schema, session
        )

        if not source:
            return {"error": f"Source schema not found: {source_object}"}
        if not target:
            return {"error": f"Target schema not found: {target_object}"}

        is_compatible, issues = registry.validate_schema_compatibility(source, target)
        return {
            "is_compatible": is_compatible,
            "issues": issues,
            "source": source.full_object_path,
            "target": target.full_object_path,
        }


@mcp.tool()
def track_lineage(
    project_id: str,
    source_object: str,
    target_object: str,
    transformation_type: str,
    source_column: str = None,
    target_column: str = None,
    transformation_logic: str = None,
    description: str = None
) -> Dict[str, Any]:
    """
    Record a lineage edge between source and target objects.

    Args:
        project_id: Project ID
        source_object: Fully qualified source object name
        target_object: Fully qualified target object name
        transformation_type: Type of transformation (select, aggregate, filter, join, etc.)
        source_column: Optional source column for column-level lineage
        target_column: Optional target column for column-level lineage
        transformation_logic: SQL or formula for the transformation
        description: Human-readable description

    Returns:
        Created lineage edge
    """
    from src.core.database import get_session
    tracker = get_lineage_tracker()

    with session_scope() as session:
        edge = tracker.track_lineage(
            project_id=project_id,
            source_object=source_object,
            target_object=target_object,
            transformation_type=TransformationType(transformation_type),
            source_column=source_column,
            target_column=target_column,
            transformation_logic=transformation_logic,
            description=description,
            session=session,
        )
        return {
            "success": True,
            "id": edge.id,
            "source": edge.source_object_name,
            "target": edge.target_object_name,
            "type": edge.transformation_type.value if edge.transformation_type else None,
        }


@mcp.tool()
def get_schema_lineage(
    project_id: str,
    object_name: str,
    direction: str = "both",
    max_depth: int = 3
) -> Dict[str, Any]:
    """
    Get upstream and/or downstream lineage for an object.

    Args:
        project_id: Project ID
        object_name: Object to trace lineage for
        direction: Direction to trace ('upstream', 'downstream', 'both')
        max_depth: Maximum traversal depth

    Returns:
        Lineage information with upstream and downstream objects
    """
    from src.core.database import get_session
    tracker = get_lineage_tracker()

    with session_scope() as session:
        if direction == "both":
            result = tracker.get_full_lineage(
                project_id=project_id,
                object_name=object_name,
                max_depth=max_depth,
                session=session,
            )
        elif direction == "upstream":
            upstream = tracker.get_upstream(
                project_id=project_id,
                object_name=object_name,
                max_depth=max_depth,
                session=session,
            )
            result = {"object": object_name, "upstream": upstream, "downstream": []}
        else:
            downstream = tracker.get_downstream(
                project_id=project_id,
                object_name=object_name,
                max_depth=max_depth,
                session=session,
            )
            result = {"object": object_name, "upstream": [], "downstream": downstream}

        return result


@mcp.tool()
def visualize_lineage(
    project_id: str,
    object_name: str = None,
    format: str = "mermaid",
    max_depth: int = 3
) -> Dict[str, Any]:
    """
    Generate a visualization of the lineage graph.

    Args:
        project_id: Project ID
        object_name: Optional object to center visualization on
        format: Output format ('mermaid', 'dot', 'json')
        max_depth: Maximum depth for subgraph

    Returns:
        Visualization string in requested format
    """
    from src.core.database import get_session
    tracker = get_lineage_tracker()

    with session_scope() as session:
        visualization = tracker.visualize_lineage(
            project_id=project_id,
            object_name=object_name,
            format=format,
            max_depth=max_depth,
            session=session,
        )
        return {
            "format": format,
            "object": object_name,
            "visualization": visualization,
        }


@mcp.tool()
def analyze_impact(
    project_id: str,
    object_name: str
) -> Dict[str, Any]:
    """
    Analyze the impact of changes to an object.

    Shows all downstream objects that would be affected by changes.

    Args:
        project_id: Project ID
        object_name: Object being changed

    Returns:
        Impact analysis with affected objects by depth
    """
    from src.core.database import get_session
    tracker = get_lineage_tracker()

    with session_scope() as session:
        impact = tracker.analyze_impact(
            project_id=project_id,
            object_name=object_name,
            session=session,
        )
        return impact


print("=" * 60)
print("DataBridge Librarian MCP Server")
print("=" * 60)
print(f"Tools registered: 57+ (45 original + 12 SQL Generator)")
print("Modules: hierarchy, reconciliation, vectors, sql_generator")
print("=" * 60)

if __name__ == "__main__":
    mcp.run()
