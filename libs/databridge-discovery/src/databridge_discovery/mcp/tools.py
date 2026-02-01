"""
MCP Tools for DataBridge Discovery Engine.

Phase 1 - SQL Parser & Session Foundation (6 tools):
1. parse_sql - Parse SQL and return AST
2. extract_case_statements - Extract CASE WHEN logic
3. analyze_sql_complexity - Query complexity metrics
4. start_discovery_session - Initialize session
5. get_discovery_session - Get session state
6. export_discovery_evidence - Export evidence

Phase 2 - Semantic Graph & Embeddings (8 tools):
7. build_semantic_graph - Build graph from schema
8. add_graph_relationship - Add edge to graph
9. find_join_paths - Find join candidates
10. analyze_graph_centrality - Central entities
11. embed_schema_element - Generate embedding
12. search_similar_schemas - Semantic search
13. consolidate_entities - Merge duplicates
14. export_semantic_model - Export JSON/GraphML

Phase 3 - Hierarchy Extraction (10 tools):
15. extract_hierarchy_from_sql - CASE → hierarchy
16. analyze_csv_for_hierarchy - CSV analysis
17. detect_entity_types - Detect 12 entities
18. infer_hierarchy_levels - Level structure
19. generate_sort_orders - Sort order calculation
20. merge_with_librarian_hierarchy - Librarian merge
21. export_discovery_as_csv - Librarian-compatible CSV
22. validate_hierarchy_structure - Integrity check
23. suggest_parent_child_relationships - P/C suggestions
24. compare_hierarchies - Diff hierarchies

Phase 4 - Project Generation & Documentation (8 tools):
25. generate_librarian_project - Complete Librarian project
26. generate_hierarchy_from_discovery - Create hierarchy
27. generate_vw1_views - VW_1 tier views
28. generate_dbt_models - dbt model files
29. generate_data_dictionary - Auto dictionary
30. export_lineage_diagram - Mermaid/D2 diagram
31. validate_generated_project - Completeness check
32. preview_deployment_scripts - DDL preview

Phase 5 - Multi-Agent Orchestration (12 tools):
33. start_discovery_workflow - Full end-to-end workflow
34. get_workflow_status - Workflow progress
35. invoke_schema_scanner - Scanner agent
36. invoke_logic_extractor - Extractor agent
37. invoke_warehouse_architect - Architect agent
38. invoke_deploy_validator - Deploy agent
39. pause_workflow - Pause execution
40. resume_workflow - Resume execution
41. get_agent_capabilities - List capabilities
42. configure_agent - Configure params
43. validate_workflow_config - Validate config
44. get_workflow_history - Past runs
"""

from __future__ import annotations

from typing import Any

# Global session storage (in production, use proper persistence)
_sessions: dict[str, Any] = {}

# Global semantic graph storage
_graphs: dict[str, Any] = {}

# Global similarity search storage
_similarity_indexes: dict[str, Any] = {}

# Global hierarchy storage
_hierarchies: dict[str, Any] = {}

# Global generated projects storage
_generated_projects: dict[str, Any] = {}

# Global orchestrator storage
_orchestrators: dict[str, Any] = {}

# Global workflow execution storage
_workflow_executions: dict[str, Any] = {}


def register_discovery_tools(mcp_server: Any) -> None:
    """
    Register all discovery tools with an MCP server.

    Registers 44 tools across 5 phases:
    - Phase 1: SQL Parser & Session Foundation (6 tools)
    - Phase 2: Semantic Graph & Embeddings (8 tools)
    - Phase 3: Hierarchy Extraction (10 tools)
    - Phase 4: Project Generation & Documentation (8 tools)
    - Phase 5: Multi-Agent Orchestration (12 tools)

    Args:
        mcp_server: FastMCP server instance
    """
    # Phase 1 Tools
    from databridge_discovery.mcp.tools import (
        parse_sql,
        extract_case_statements,
        analyze_sql_complexity,
        start_discovery_session,
        get_discovery_session,
        export_discovery_evidence,
    )

    # Phase 2 Tools
    from databridge_discovery.mcp.tools import (
        build_semantic_graph,
        add_graph_relationship,
        find_join_paths,
        analyze_graph_centrality,
        embed_schema_element,
        search_similar_schemas,
        consolidate_entities,
        export_semantic_model,
    )

    # Phase 3 Tools
    from databridge_discovery.mcp.tools import (
        extract_hierarchy_from_sql,
        analyze_csv_for_hierarchy,
        detect_entity_types,
        infer_hierarchy_levels,
        generate_sort_orders,
        merge_with_librarian_hierarchy,
        export_discovery_as_csv,
        validate_hierarchy_structure,
        suggest_parent_child_relationships,
        compare_hierarchies,
    )

    # Phase 4 Tools
    from databridge_discovery.mcp.tools import (
        generate_librarian_project,
        generate_hierarchy_from_discovery,
        generate_vw1_views,
        generate_dbt_models,
        generate_data_dictionary,
        export_lineage_diagram,
        validate_generated_project,
        preview_deployment_scripts,
    )

    # Phase 5 Tools
    from databridge_discovery.mcp.tools import (
        start_discovery_workflow,
        get_workflow_status,
        invoke_schema_scanner,
        invoke_logic_extractor,
        invoke_warehouse_architect,
        invoke_deploy_validator,
        pause_workflow,
        resume_workflow,
        get_agent_capabilities,
        configure_agent,
        validate_workflow_config,
        get_workflow_history,
    )

    # Tools are registered via decorators, but this function
    # can be used to programmatically add them if needed
    pass


def parse_sql(
    sql: str,
    dialect: str = "snowflake",
) -> dict[str, Any]:
    """
    Parse SQL statement and return structured AST information.

    This tool parses a SQL statement using sqlglot and returns detailed
    information about tables, columns, joins, and query structure.

    Args:
        sql: SQL statement to parse
        dialect: SQL dialect (snowflake, postgres, tsql, mysql, bigquery)

    Returns:
        Dictionary containing:
        - query_type: Type of SQL statement (SELECT, INSERT, etc.)
        - tables: List of table references with schema info
        - columns: List of columns in SELECT with metadata
        - joins: List of join relationships
        - ctes: Common Table Expressions
        - metrics: Query complexity metrics
        - parse_errors: Any parsing errors

    Example:
        >>> result = parse_sql("SELECT * FROM users WHERE active = 1")
        >>> print(result["tables"])
        [{"name": "users", "alias": null}]
    """
    from databridge_discovery.parser.sql_parser import SQLParser

    parser = SQLParser(dialect=dialect)
    parsed = parser.parse(sql)

    return {
        "query_type": parsed.query_type,
        "dialect": parsed.dialect,
        "tables": [
            {
                "name": t.name,
                "schema": t.schema_name,
                "database": t.database,
                "alias": t.alias,
                "is_subquery": t.is_subquery,
                "is_cte": t.is_cte,
            }
            for t in parsed.tables
        ],
        "columns": [
            {
                "name": c.name,
                "source_name": c.source_name,
                "table_ref": c.table_ref,
                "data_type": c.data_type.value,
                "is_derived": c.is_derived,
                "is_case_statement": c.is_case_statement,
                "aggregation": c.aggregation.value if c.aggregation else None,
                "position": c.position,
            }
            for c in parsed.columns
        ],
        "joins": [
            {
                "join_type": j.join_type.value,
                "left_table": j.left_table,
                "right_table": j.right_table,
                "left_column": j.left_column,
                "right_column": j.right_column,
                "condition": j.condition,
            }
            for j in parsed.joins
        ],
        "ctes": parsed.ctes,
        "where_clause": parsed.where_clause,
        "group_by": parsed.group_by_columns,
        "order_by": parsed.order_by_columns,
        "metrics": {
            "table_count": parsed.metrics.table_count,
            "join_count": parsed.metrics.join_count,
            "column_count": parsed.metrics.column_count,
            "case_statement_count": parsed.metrics.case_statement_count,
            "subquery_count": parsed.metrics.subquery_count,
            "cte_count": parsed.metrics.cte_count,
            "aggregation_count": parsed.metrics.aggregation_count,
            "has_group_by": parsed.metrics.has_group_by,
            "has_window_functions": parsed.metrics.has_window_functions,
            "estimated_complexity": parsed.metrics.estimated_complexity,
            "nesting_depth": parsed.metrics.nesting_depth,
        },
        "parse_time_ms": parsed.parse_time_ms,
        "parse_errors": parsed.parse_errors,
    }


def extract_case_statements(
    sql: str,
    dialect: str = "snowflake",
    include_hierarchy: bool = True,
) -> dict[str, Any]:
    """
    Extract CASE WHEN statements from SQL with hierarchy detection.

    This tool analyzes SQL to find all CASE statements, extract their
    conditions and results, detect entity types, and identify potential
    hierarchies that can be built from the CASE logic.

    Args:
        sql: SQL containing CASE statements
        dialect: SQL dialect (snowflake, postgres, tsql, mysql, bigquery)
        include_hierarchy: Also extract hierarchy proposals from CASE logic

    Returns:
        Dictionary containing:
        - case_count: Number of CASE statements found
        - case_statements: List of extracted CASE statements with:
          - id: Unique identifier
          - source_column: Column name this CASE creates
          - input_column: Column being tested
          - entity_type: Detected entity type (account, cost_center, etc.)
          - pattern: Detected pattern (prefix, suffix, exact, etc.)
          - condition_count: Number of WHEN clauses
          - unique_results: Unique THEN values
        - hierarchies: Proposed hierarchies (if include_hierarchy=True)

    Example:
        >>> sql = '''
        ... SELECT
        ...   CASE WHEN account_code LIKE '5%' THEN 'Revenue'
        ...        WHEN account_code LIKE '6%' THEN 'Expenses'
        ...   END as category
        ... FROM gl_entries
        ... '''
        >>> result = extract_case_statements(sql)
        >>> print(result["case_statements"][0]["entity_type"])
        "account"
    """
    from databridge_discovery.parser.case_extractor import CaseExtractor

    extractor = CaseExtractor(dialect=dialect)
    cases = extractor.extract_from_sql(sql)

    result = {
        "case_count": len(cases),
        "case_statements": [],
        "hierarchies": [] if include_hierarchy else None,
    }

    for case in cases:
        case_info = {
            "id": case.id,
            "source_column": case.source_column,
            "input_column": case.input_column,
            "input_table": case.input_table,
            "entity_type": case.detected_entity_type.value,
            "pattern": case.detected_pattern,
            "condition_count": case.condition_count,
            "unique_results": case.unique_result_values,
            "when_clauses": [
                {
                    "condition": w.condition.raw_condition,
                    "operator": w.condition.operator.value,
                    "values": w.condition.values,
                    "result": w.result_value,
                }
                for w in case.when_clauses
            ],
            "else_value": case.else_value,
        }
        result["case_statements"].append(case_info)

        # Extract hierarchy if requested
        if include_hierarchy:
            hierarchy = extractor.extract_hierarchy(case)
            if hierarchy:
                result["hierarchies"].append({
                    "id": hierarchy.id,
                    "name": hierarchy.name,
                    "entity_type": hierarchy.entity_type.value,
                    "source_column": hierarchy.source_column,
                    "level_count": hierarchy.total_levels,
                    "node_count": hierarchy.total_nodes,
                    "confidence": hierarchy.confidence_score,
                    "confidence_notes": hierarchy.confidence_notes,
                    "levels": [
                        {
                            "level_number": l.level_number,
                            "level_name": l.level_name,
                            "values": l.values,
                        }
                        for l in hierarchy.levels
                    ],
                })

    return result


def analyze_sql_complexity(
    sql: str,
    dialect: str = "snowflake",
) -> dict[str, Any]:
    """
    Analyze SQL query complexity and provide metrics.

    This tool provides detailed complexity analysis including table count,
    join patterns, CASE statements, subqueries, and overall complexity score.

    Args:
        sql: SQL statement to analyze
        dialect: SQL dialect (snowflake, postgres, tsql, mysql, bigquery)

    Returns:
        Dictionary containing:
        - complexity_score: Numeric complexity score
        - complexity_level: simple, moderate, or complex
        - metrics: Detailed metrics breakdown
        - recommendations: Suggestions for query optimization
        - lineage_summary: Column lineage information

    Example:
        >>> result = analyze_sql_complexity('''
        ...   SELECT a.*, b.name
        ...   FROM orders a
        ...   JOIN customers b ON a.customer_id = b.id
        ...   WHERE a.status = 'active'
        ... ''')
        >>> print(result["complexity_level"])
        "simple"
    """
    from databridge_discovery.parser.sql_parser import SQLParser
    from databridge_discovery.parser.column_resolver import ColumnResolver

    parser = SQLParser(dialect=dialect)
    parsed = parser.parse(sql)

    resolver = ColumnResolver(dialect=dialect)
    lineages = resolver.resolve(parsed)

    # Calculate complexity score
    score = (
        parsed.metrics.table_count * 1
        + parsed.metrics.join_count * 2
        + parsed.metrics.case_statement_count * 2
        + parsed.metrics.subquery_count * 3
        + parsed.metrics.cte_count * 2
        + parsed.metrics.nesting_depth * 3
    )

    # Generate recommendations
    recommendations = []

    if parsed.metrics.subquery_count > 2:
        recommendations.append("Consider using CTEs instead of deeply nested subqueries")

    if parsed.metrics.case_statement_count > 5:
        recommendations.append("Many CASE statements - consider extracting to lookup tables")

    if parsed.metrics.join_count > 5:
        recommendations.append("Many joins - verify indexes exist on join columns")

    if not parsed.metrics.has_group_by and parsed.metrics.aggregation_count > 0:
        recommendations.append("Aggregations without GROUP BY - verify intended behavior")

    if parsed.metrics.nesting_depth > 3:
        recommendations.append("Deep nesting - consider refactoring for readability")

    return {
        "complexity_score": score,
        "complexity_level": parsed.metrics.estimated_complexity,
        "metrics": {
            "table_count": parsed.metrics.table_count,
            "join_count": parsed.metrics.join_count,
            "column_count": parsed.metrics.column_count,
            "case_statement_count": parsed.metrics.case_statement_count,
            "subquery_count": parsed.metrics.subquery_count,
            "cte_count": parsed.metrics.cte_count,
            "aggregation_count": parsed.metrics.aggregation_count,
            "nesting_depth": parsed.metrics.nesting_depth,
            "has_group_by": parsed.metrics.has_group_by,
            "has_having": parsed.metrics.has_having,
            "has_order_by": parsed.metrics.has_order_by,
            "has_union": parsed.metrics.has_union,
            "has_window_functions": parsed.metrics.has_window_functions,
        },
        "recommendations": recommendations,
        "lineage_summary": {
            "output_columns": len(lineages),
            "source_tables": list(set(
                s.table_name
                for l in lineages
                for s in l.source_columns
                if s.table_name
            )),
            "derived_columns": len([l for l in lineages if l.output_column.is_derived]),
            "aggregated_columns": len([l for l in lineages if l.is_aggregated]),
            "case_derived_columns": len([l for l in lineages if l.is_case_derived]),
        },
        "parse_time_ms": parsed.parse_time_ms,
    }


def start_discovery_session(
    name: str = "Untitled Discovery",
    dialect: str = "snowflake",
    sql_sources: list[str] | None = None,
    persist_path: str | None = None,
) -> dict[str, Any]:
    """
    Start a new discovery session for analyzing SQL sources.

    A discovery session tracks all sources analyzed, CASE statements found,
    proposed hierarchies, and user decisions. Sessions can be persisted
    for later resumption.

    Args:
        name: Name for this discovery session
        dialect: SQL dialect for parsing (snowflake, postgres, tsql, mysql, bigquery)
        sql_sources: Optional list of SQL strings to analyze immediately
        persist_path: Path to SQLite file for session persistence

    Returns:
        Dictionary containing:
        - session_id: Unique session identifier
        - name: Session name
        - status: Current status (created, analyzing, reviewed, etc.)
        - dialect: SQL dialect being used
        - sources_added: Number of sources added
        - analysis_complete: Whether initial analysis is done

    Example:
        >>> result = start_discovery_session(
        ...     name="Q1 GL Analysis",
        ...     sql_sources=["SELECT * FROM gl_entries"]
        ... )
        >>> print(result["session_id"])
        "abc123..."
    """
    from databridge_discovery.session.discovery_session import DiscoverySession

    session = DiscoverySession(
        name=name,
        dialect=dialect,
        persist_path=persist_path,
    )

    # Add SQL sources if provided
    if sql_sources:
        for idx, sql in enumerate(sql_sources):
            session.add_sql_source(sql, f"source_{idx}")

        # Run analysis
        session.analyze()

    # Store session globally
    _sessions[session.id] = session

    return {
        "session_id": session.id,
        "name": name,
        "status": session.status.value,
        "dialect": dialect,
        "sources_added": len(sql_sources) if sql_sources else 0,
        "analysis_complete": bool(sql_sources),
        "summary": session.get_summary(),
    }


def get_discovery_session(
    session_id: str,
    include_proposals: bool = True,
    include_evidence: bool = False,
) -> dict[str, Any]:
    """
    Get the current state of a discovery session.

    Retrieves detailed information about a discovery session including
    sources analyzed, proposed hierarchies, and approval status.

    Args:
        session_id: ID of the session to retrieve
        include_proposals: Include proposed hierarchies in response
        include_evidence: Include collected evidence in response

    Returns:
        Dictionary containing:
        - session_id: Session identifier
        - name: Session name
        - status: Current status
        - summary: Session statistics
        - sources: List of analyzed sources
        - proposals: Proposed hierarchies (if include_proposals=True)
        - evidence: Collected evidence (if include_evidence=True)

    Example:
        >>> result = get_discovery_session("abc123")
        >>> print(result["summary"]["case_statements_found"])
        42
    """
    if session_id not in _sessions:
        return {
            "error": f"Session not found: {session_id}",
            "available_sessions": list(_sessions.keys()),
        }

    session = _sessions[session_id]
    state = session.state

    result = {
        "session_id": session.id,
        "name": state.name,
        "status": state.status.value,
        "dialect": state.target_dialect,
        "created_at": state.created_at.isoformat(),
        "updated_at": state.updated_at.isoformat(),
        "summary": state.to_summary(),
        "sources": [
            {
                "id": s.id,
                "type": s.source_type.value,
                "name": s.source_name,
                "path": s.source_path,
                "rows": s.row_count,
                "columns": s.column_count,
            }
            for s in state.sources
        ],
    }

    if include_proposals:
        result["proposals"] = [
            {
                "id": p.id,
                "name": p.name,
                "status": p.status,
                "entity_type": p.detected_entity_type,
                "confidence": p.entity_confidence,
                "level_count": p.level_count,
                "node_count": p.node_count,
                "source_column": p.source_column,
            }
            for p in state.proposed_hierarchies
        ]

    if include_evidence:
        result["evidence"] = [
            {
                "id": e.id,
                "type": e.evidence_type.value,
                "title": e.title,
                "confidence": e.confidence,
                "hierarchy_id": e.hierarchy_id,
            }
            for e in state.evidence
        ]

    return result


def export_discovery_evidence(
    session_id: str,
    output_path: str,
    format: str = "json",
    include_case_sql: bool = True,
) -> dict[str, Any]:
    """
    Export evidence collected during discovery to a file.

    Exports all evidence supporting hierarchy proposals, including
    CASE statements, patterns detected, and confidence scores.

    Args:
        session_id: ID of the session to export
        output_path: Path to write the export file
        format: Export format (json, csv)
        include_case_sql: Include raw CASE statement SQL in export

    Returns:
        Dictionary containing:
        - success: Whether export succeeded
        - output_path: Path to created file
        - evidence_count: Number of evidence items exported
        - case_statements: Number of CASE statements included

    Example:
        >>> result = export_discovery_evidence(
        ...     session_id="abc123",
        ...     output_path="evidence.json"
        ... )
        >>> print(result["output_path"])
        "evidence.json"
    """
    if session_id not in _sessions:
        return {
            "success": False,
            "error": f"Session not found: {session_id}",
        }

    session = _sessions[session_id]

    try:
        if format == "json":
            output_file = session.export_evidence(output_path)
        else:
            # CSV export
            import csv
            from pathlib import Path

            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            evidence = session.get_evidence()
            if evidence:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    fieldnames = ["id", "type", "title", "description", "confidence", "hierarchy_id"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for e in evidence:
                        writer.writerow({
                            "id": e.id,
                            "type": e.evidence_type.value,
                            "title": e.title,
                            "description": e.description,
                            "confidence": e.confidence,
                            "hierarchy_id": e.hierarchy_id,
                        })
            output_file = str(path)

        return {
            "success": True,
            "output_path": output_file,
            "evidence_count": len(session.get_evidence()),
            "case_statements": len(session.get_case_statements()),
            "format": format,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


# Additional utility functions for session management

def add_sql_to_session(
    session_id: str,
    sql: str,
    source_name: str = "inline_sql",
    analyze: bool = True,
) -> dict[str, Any]:
    """
    Add SQL source to an existing session.

    Args:
        session_id: Session ID
        sql: SQL to add
        source_name: Name for this source
        analyze: Run analysis after adding

    Returns:
        Dictionary with source info and analysis results
    """
    if session_id not in _sessions:
        return {"error": f"Session not found: {session_id}"}

    session = _sessions[session_id]
    source = session.add_sql_source(sql, source_name)

    result = {
        "source_id": source.id,
        "source_name": source_name,
        "added": True,
    }

    if analyze:
        analysis = session.analyze()
        result["analysis"] = analysis

    return result


def approve_hierarchy(
    session_id: str,
    hierarchy_id: str,
) -> dict[str, Any]:
    """
    Approve a proposed hierarchy in a session.

    Args:
        session_id: Session ID
        hierarchy_id: Hierarchy ID to approve

    Returns:
        Dictionary with approval status
    """
    if session_id not in _sessions:
        return {"error": f"Session not found: {session_id}"}

    session = _sessions[session_id]
    result = session.approve_hierarchy(hierarchy_id)

    return {
        "approved": result,
        "hierarchy_id": hierarchy_id,
        "session_summary": session.get_summary(),
    }


def reject_hierarchy(
    session_id: str,
    hierarchy_id: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """
    Reject a proposed hierarchy in a session.

    Args:
        session_id: Session ID
        hierarchy_id: Hierarchy ID to reject
        reason: Optional rejection reason

    Returns:
        Dictionary with rejection status
    """
    if session_id not in _sessions:
        return {"error": f"Session not found: {session_id}"}

    session = _sessions[session_id]
    result = session.reject_hierarchy(hierarchy_id, reason)

    return {
        "rejected": result,
        "hierarchy_id": hierarchy_id,
        "reason": reason,
        "session_summary": session.get_summary(),
    }


def export_librarian_csv(
    session_id: str,
    output_dir: str,
) -> dict[str, Any]:
    """
    Export approved hierarchies to Librarian-compatible CSV files.

    Args:
        session_id: Session ID
        output_dir: Directory for CSV files

    Returns:
        Dictionary with export results
    """
    if session_id not in _sessions:
        return {"error": f"Session not found: {session_id}"}

    session = _sessions[session_id]
    files = session.export_to_librarian_csv(output_dir)

    return {
        "success": True,
        "output_dir": output_dir,
        "files_created": files,
        "file_count": len(files),
    }


# =============================================================================
# Phase 2 Tools: Semantic Graph & Embeddings
# =============================================================================


def build_semantic_graph(
    name: str = "default",
    from_session_id: str | None = None,
    from_sql: str | None = None,
    dialect: str = "snowflake",
) -> dict[str, Any]:
    """
    Build a semantic graph from schema metadata or parsed SQL.

    Creates a graph representing tables, columns, and their relationships
    that can be used for join path discovery, centrality analysis, and
    semantic search.

    Args:
        name: Name for the graph (used as identifier)
        from_session_id: Build from an existing discovery session
        from_sql: Build from SQL statement
        dialect: SQL dialect for parsing

    Returns:
        Dictionary containing:
        - graph_id: Unique graph identifier
        - name: Graph name
        - stats: Graph statistics (nodes, edges, types)
        - tables: List of table nodes
        - columns: Number of column nodes

    Example:
        >>> result = build_semantic_graph(
        ...     name="sales_model",
        ...     from_sql="SELECT * FROM orders JOIN customers ON ..."
        ... )
        >>> print(result["stats"]["node_count"])
        25
    """
    from databridge_discovery.graph.semantic_graph import SemanticGraph
    from databridge_discovery.parser.sql_parser import SQLParser

    graph = SemanticGraph(name=name)

    if from_session_id and from_session_id in _sessions:
        session = _sessions[from_session_id]
        # Add from session's parsed queries
        for source in session.state.sources:
            if source.content:
                parser = SQLParser(dialect=dialect)
                parsed = parser.parse(source.content)
                if not parsed.parse_errors:
                    graph.add_from_parsed_query(parsed)

    elif from_sql:
        parser = SQLParser(dialect=dialect)
        parsed = parser.parse(from_sql)
        if not parsed.parse_errors:
            graph.add_from_parsed_query(parsed)

    # Store the graph
    _graphs[name] = graph
    stats = graph.get_stats()

    return {
        "graph_id": name,
        "name": name,
        "stats": stats.model_dump(),
        "tables": [
            {"id": n.id, "name": n.name, "full_name": n.full_name}
            for n in graph.get_nodes_by_type("table")
        ],
        "column_count": stats.column_count,
    }


def add_graph_relationship(
    graph_id: str,
    source_node: str,
    target_node: str,
    edge_type: str,
    weight: float = 1.0,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Add a relationship (edge) to a semantic graph.

    Adds a directed edge between two nodes in the graph with the
    specified relationship type and optional metadata.

    Args:
        graph_id: ID of the graph to modify
        source_node: Source node ID or name
        target_node: Target node ID or name
        edge_type: Type of relationship (foreign_key, join, similar_to, etc.)
        weight: Edge weight (0.0 to 1.0)
        metadata: Additional edge metadata

    Returns:
        Dictionary containing:
        - success: Whether the edge was added
        - edge_id: ID of the created edge
        - source: Source node info
        - target: Target node info
        - edge_type: Relationship type

    Example:
        >>> result = add_graph_relationship(
        ...     graph_id="sales_model",
        ...     source_node="orders",
        ...     target_node="customers",
        ...     edge_type="foreign_key"
        ... )
        >>> print(result["success"])
        True
    """
    from databridge_discovery.graph.node_types import EdgeType, GraphEdge

    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]

    # Validate edge type
    try:
        edge_type_enum = EdgeType(edge_type)
    except ValueError:
        return {
            "error": f"Invalid edge type: {edge_type}",
            "valid_types": [e.value for e in EdgeType],
        }

    # Find nodes by ID or name
    source = graph.get_node(source_node)
    if not source:
        nodes = graph.find_nodes_by_name(source_node)
        source = nodes[0] if nodes else None

    target = graph.get_node(target_node)
    if not target:
        nodes = graph.find_nodes_by_name(target_node)
        target = nodes[0] if nodes else None

    if not source:
        return {"error": f"Source node not found: {source_node}"}
    if not target:
        return {"error": f"Target node not found: {target_node}"}

    # Create and add edge
    edge = GraphEdge(
        edge_type=edge_type_enum,
        source_id=source.id,
        target_id=target.id,
        weight=weight,
        metadata=metadata or {},
    )

    graph.add_edge(edge)

    return {
        "success": True,
        "edge_id": edge.id,
        "source": {"id": source.id, "name": source.name},
        "target": {"id": target.id, "name": target.name},
        "edge_type": edge_type,
        "weight": weight,
    }


def find_join_paths(
    graph_id: str,
    source_table: str,
    target_table: str,
    max_length: int = 5,
) -> dict[str, Any]:
    """
    Find all possible join paths between two tables.

    Analyzes the semantic graph to find paths connecting two tables,
    useful for query optimization and data lineage analysis.

    Args:
        graph_id: ID of the graph to search
        source_table: Source table name
        target_table: Target table name
        max_length: Maximum path length (number of joins)

    Returns:
        Dictionary containing:
        - paths: List of join paths with:
          - tables: Tables in the path
          - length: Number of joins
          - join_conditions: Suggested join conditions
          - confidence: Path confidence score
        - shortest_path: The shortest path found
        - path_count: Total paths found

    Example:
        >>> result = find_join_paths(
        ...     graph_id="sales_model",
        ...     source_table="orders",
        ...     target_table="products"
        ... )
        >>> print(result["paths"][0]["tables"])
        ["orders", "order_items", "products"]
    """
    from databridge_discovery.graph.graph_analyzer import GraphAnalyzer

    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]
    analyzer = GraphAnalyzer(graph)

    paths = analyzer.find_join_paths(source_table, target_table, max_length)

    result = {
        "source": source_table,
        "target": target_table,
        "paths": [],
        "shortest_path": None,
        "path_count": len(paths),
    }

    for path in paths:
        path_info = {
            "tables": path.path,
            "length": path.length,
            "join_conditions": path.join_conditions,
            "confidence": path.confidence,
        }
        result["paths"].append(path_info)

        if result["shortest_path"] is None or path.length < result["shortest_path"]["length"]:
            result["shortest_path"] = path_info

    return result


def analyze_graph_centrality(
    graph_id: str,
    algorithm: str = "degree",
    node_type: str | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    """
    Analyze node centrality to find important entities.

    Calculates centrality metrics to identify the most important or
    connected nodes in the semantic graph.

    Args:
        graph_id: ID of the graph to analyze
        algorithm: Centrality algorithm (degree, betweenness, pagerank, eigenvector)
        node_type: Filter by node type (table, column, hierarchy)
        top_k: Number of top results to return

    Returns:
        Dictionary containing:
        - algorithm: Algorithm used
        - results: List of centrality results with:
          - node_id: Node identifier
          - name: Node name
          - type: Node type
          - score: Centrality score
          - rank: Ranking position
        - summary: Analysis summary

    Example:
        >>> result = analyze_graph_centrality(
        ...     graph_id="sales_model",
        ...     algorithm="pagerank",
        ...     node_type="table"
        ... )
        >>> print(result["results"][0]["name"])
        "orders"
    """
    from databridge_discovery.graph.graph_analyzer import GraphAnalyzer
    from databridge_discovery.graph.node_types import NodeType

    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]
    analyzer = GraphAnalyzer(graph)

    # Parse node type
    node_type_enum = None
    if node_type:
        try:
            node_type_enum = NodeType(node_type)
        except ValueError:
            return {
                "error": f"Invalid node type: {node_type}",
                "valid_types": [t.value for t in NodeType],
            }

    # Run centrality analysis
    if algorithm == "degree":
        results = analyzer.get_degree_centrality(node_type_enum, top_k)
    elif algorithm == "betweenness":
        results = analyzer.get_betweenness_centrality(node_type_enum, top_k)
    elif algorithm == "pagerank":
        results = analyzer.get_pagerank(node_type_enum, top_k)
    elif algorithm == "eigenvector":
        results = analyzer.get_eigenvector_centrality(node_type_enum, top_k)
    else:
        return {
            "error": f"Invalid algorithm: {algorithm}",
            "valid_algorithms": ["degree", "betweenness", "pagerank", "eigenvector"],
        }

    return {
        "algorithm": algorithm,
        "node_type_filter": node_type,
        "results": [
            {
                "node_id": r.node_id,
                "name": r.node_name,
                "type": r.node_type,
                "score": r.score,
                "rank": r.rank,
            }
            for r in results
        ],
        "summary": {
            "top_node": results[0].node_name if results else None,
            "result_count": len(results),
            "max_score": results[0].score if results else 0,
        },
    }


def embed_schema_element(
    element_type: str,
    name: str,
    description: str | None = None,
    additional_context: dict[str, Any] | None = None,
    graph_id: str | None = None,
) -> dict[str, Any]:
    """
    Generate vector embedding for a schema element.

    Creates a vector embedding that can be used for semantic search
    and similarity matching.

    Args:
        element_type: Type of element (table, column, hierarchy)
        name: Element name
        description: Optional description text
        additional_context: Additional context for embedding
        graph_id: Optionally add to an existing graph's index

    Returns:
        Dictionary containing:
        - element_type: Type of element
        - name: Element name
        - embedding_dim: Embedding dimension
        - embedding_preview: First 10 values of embedding
        - model_info: Embedding model information
        - indexed: Whether added to graph index

    Example:
        >>> result = embed_schema_element(
        ...     element_type="table",
        ...     name="customer_orders",
        ...     description="Contains all customer order history"
        ... )
        >>> print(result["embedding_dim"])
        384
    """
    from databridge_discovery.embeddings.schema_embedder import SchemaEmbedder
    from databridge_discovery.graph.node_types import (
        ColumnNode,
        HierarchyNode,
        NodeType,
        TableNode,
    )

    embedder = SchemaEmbedder()

    # Create appropriate node type
    if element_type == "table":
        node = TableNode(
            name=name,
            table_name=name,
            description=description,
            metadata=additional_context or {},
        )
        embedding = embedder.embed_table(node)
    elif element_type == "column":
        node = ColumnNode(
            name=name,
            column_name=name,
            description=description,
            data_type=additional_context.get("data_type", "unknown") if additional_context else "unknown",
        )
        embedding = embedder.embed_column(node)
    elif element_type == "hierarchy":
        node = HierarchyNode(
            name=name,
            description=description,
            source_column=additional_context.get("source_column") if additional_context else None,
        )
        embedding = embedder.embed_hierarchy(node)
    else:
        # Generic text embedding
        text = f"{element_type} {name}"
        if description:
            text += f" {description}"
        embedding = embedder.embed_text(text)
        node = None

    result = {
        "element_type": element_type,
        "name": name,
        "embedding_dim": len(embedding),
        "embedding_preview": embedding[:10],
        "model_info": embedder.get_model_info(),
        "indexed": False,
    }

    # Add to graph's similarity index if requested
    if graph_id and graph_id in _graphs and node:
        if graph_id not in _similarity_indexes:
            from databridge_discovery.embeddings.similarity import SimilaritySearch
            _similarity_indexes[graph_id] = SimilaritySearch(embedder=embedder)

        node.embedding = embedding
        _similarity_indexes[graph_id].index_node(node)
        result["indexed"] = True
        result["graph_id"] = graph_id

    return result


def search_similar_schemas(
    query: str,
    graph_id: str | None = None,
    node_type: str | None = None,
    top_k: int = 10,
    threshold: float = 0.5,
) -> dict[str, Any]:
    """
    Search for schema elements similar to a query.

    Performs semantic search using vector embeddings to find
    tables, columns, or hierarchies similar to the query text.

    Args:
        query: Search query text
        graph_id: Search within a specific graph's index
        node_type: Filter by node type (table, column, hierarchy)
        top_k: Number of results to return
        threshold: Minimum similarity threshold (0.0 to 1.0)

    Returns:
        Dictionary containing:
        - query: Original query
        - results: List of matching elements with:
          - node_id: Node identifier
          - name: Element name
          - type: Element type
          - score: Similarity score
          - description: Element description
        - result_count: Number of results found

    Example:
        >>> result = search_similar_schemas(
        ...     query="customer billing information",
        ...     node_type="table"
        ... )
        >>> print(result["results"][0]["name"])
        "customer_invoices"
    """
    from databridge_discovery.embeddings.schema_embedder import SchemaEmbedder
    from databridge_discovery.embeddings.similarity import SimilaritySearch

    # Get or create similarity index
    if graph_id and graph_id in _similarity_indexes:
        search = _similarity_indexes[graph_id]
    elif graph_id and graph_id in _graphs:
        # Create index from graph
        embedder = SchemaEmbedder()
        search = SimilaritySearch(embedder=embedder)
        graph = _graphs[graph_id]

        # Index all nodes from graph
        nodes = graph.get_all_nodes()
        search.index_nodes(nodes)
        _similarity_indexes[graph_id] = search
    else:
        return {
            "error": "No graph or index specified",
            "available_graphs": list(_graphs.keys()),
            "available_indexes": list(_similarity_indexes.keys()),
        }

    # Perform search
    results = search.search_text(
        query=query,
        top_k=top_k,
        node_type=node_type,
        threshold=threshold,
    )

    return {
        "query": query,
        "node_type_filter": node_type,
        "results": [
            {
                "node_id": r.node_id,
                "name": r.node_name,
                "type": r.node_type,
                "score": r.score,
                "metadata": r.metadata,
            }
            for r in results
        ],
        "result_count": len(results),
        "threshold": threshold,
    }


def consolidate_entities(
    graph_id: str,
    similarity_threshold: float = 0.85,
    node_type: str | None = None,
    auto_merge: bool = False,
) -> dict[str, Any]:
    """
    Find and consolidate duplicate or similar entities.

    Analyzes the graph to find entities that may represent the same
    concept and provides merge suggestions or performs automatic merging.

    Args:
        graph_id: ID of the graph to consolidate
        similarity_threshold: Minimum similarity for merge candidates
        node_type: Filter by node type (table, column, hierarchy)
        auto_merge: Automatically merge high-confidence duplicates

    Returns:
        Dictionary containing:
        - candidates: List of merge candidates with:
          - nodes: Nodes that could be merged
          - canonical_name: Suggested canonical name
          - confidence: Merge confidence score
          - reason: Why these are candidates
        - merged: List of merged concepts (if auto_merge=True)
        - summary: Consolidation summary

    Example:
        >>> result = consolidate_entities(
        ...     graph_id="sales_model",
        ...     similarity_threshold=0.9,
        ...     auto_merge=True
        ... )
        >>> print(result["summary"]["reduction_percent"])
        35.5
    """
    from databridge_discovery.consolidation.concept_merger import ConceptMerger
    from databridge_discovery.consolidation.entity_matcher import EntityMatcher

    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]

    # Get nodes to analyze
    if node_type:
        nodes = graph.get_nodes_by_type(node_type)
    else:
        nodes = graph.get_all_nodes()

    # Create matcher and merger
    matcher = EntityMatcher(similarity_threshold=similarity_threshold)
    merger = ConceptMerger(matcher=matcher, similarity_threshold=similarity_threshold)

    # Find merge candidates
    candidates = merger.find_merge_candidates(nodes, by_type=True)

    result = {
        "graph_id": graph_id,
        "node_type_filter": node_type,
        "candidates": [
            {
                "nodes": [{"id": n.id, "name": n.name} for n in c.nodes],
                "canonical_name": c.canonical_name,
                "confidence": c.confidence,
                "reason": c.merge_reason,
                "node_count": len(c.nodes),
            }
            for c in candidates
        ],
        "merged": [],
        "summary": {},
    }

    # Auto-merge if requested
    if auto_merge:
        merge_results = merger.merge_all(candidates, auto_only=True)
        result["merged"] = [
            {
                "concept_id": m.concept.id,
                "canonical_name": m.concept.name,
                "merged_nodes": m.merged_nodes,
                "confidence": m.confidence,
                "merge_type": m.merge_type,
            }
            for m in merge_results
        ]

    # Generate summary
    result["summary"] = {
        "total_nodes_analyzed": len(nodes),
        "candidate_groups": len(candidates),
        "nodes_in_candidates": sum(len(c.nodes) for c in candidates),
        "merged_count": len(result["merged"]),
        "reduction_percent": (
            (len(result["merged"]) / max(len(nodes), 1)) * 100
            if result["merged"] else 0
        ),
    }

    return result


def export_semantic_model(
    graph_id: str,
    output_path: str,
    format: str = "json",
    include_embeddings: bool = False,
) -> dict[str, Any]:
    """
    Export semantic model to a file.

    Exports the semantic graph with nodes, edges, and optionally
    embeddings to JSON or GraphML format.

    Args:
        graph_id: ID of the graph to export
        output_path: Path to write the export file
        format: Export format (json, graphml)
        include_embeddings: Include vector embeddings in export

    Returns:
        Dictionary containing:
        - success: Whether export succeeded
        - output_path: Path to created file
        - format: Export format used
        - stats: Export statistics

    Example:
        >>> result = export_semantic_model(
        ...     graph_id="sales_model",
        ...     output_path="sales_model.json",
        ...     format="json"
        ... )
        >>> print(result["stats"]["node_count"])
        50
    """
    from pathlib import Path

    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]
    stats = graph.get_stats()

    try:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if format == "json":
            import json

            data = graph.to_json(include_embeddings=include_embeddings)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)

        elif format == "graphml":
            graphml = graph.to_graphml()
            with open(path, "w", encoding="utf-8") as f:
                f.write(graphml)

        else:
            return {
                "error": f"Unsupported format: {format}",
                "valid_formats": ["json", "graphml"],
            }

        return {
            "success": True,
            "output_path": str(path.absolute()),
            "format": format,
            "stats": {
                "node_count": stats.node_count,
                "edge_count": stats.edge_count,
                "table_count": stats.table_count,
                "column_count": stats.column_count,
                "embeddings_included": include_embeddings,
            },
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


# Graph utility functions

def get_graph_summary(graph_id: str) -> dict[str, Any]:
    """
    Get summary of a semantic graph.

    Args:
        graph_id: Graph identifier

    Returns:
        Dictionary with graph statistics
    """
    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    graph = _graphs[graph_id]
    stats = graph.get_stats()

    return {
        "graph_id": graph_id,
        "name": graph.name,
        "stats": stats.model_dump(),
    }


def list_graphs() -> dict[str, Any]:
    """
    List all available semantic graphs.

    Returns:
        Dictionary with graph list and counts
    """
    return {
        "graphs": [
            {
                "graph_id": gid,
                "name": g.name,
                "node_count": g.get_stats().node_count,
            }
            for gid, g in _graphs.items()
        ],
        "total_count": len(_graphs),
    }


def delete_graph(graph_id: str) -> dict[str, Any]:
    """
    Delete a semantic graph.

    Args:
        graph_id: Graph identifier

    Returns:
        Dictionary with deletion status
    """
    if graph_id not in _graphs:
        return {"error": f"Graph not found: {graph_id}"}

    del _graphs[graph_id]

    # Also remove similarity index if exists
    if graph_id in _similarity_indexes:
        del _similarity_indexes[graph_id]

    return {
        "success": True,
        "deleted": graph_id,
    }


# =============================================================================
# Phase 3 Tools: Hierarchy Extraction
# =============================================================================

# Global hierarchy storage
_hierarchies: dict[str, Any] = {}


def extract_hierarchy_from_sql(
    sql: str,
    dialect: str = "snowflake",
    name: str | None = None,
    session_id: str | None = None,
) -> dict[str, Any]:
    """
    Extract hierarchy structure from CASE WHEN statements in SQL.

    Parses SQL containing CASE statements and converts the logic into
    a structured hierarchy with levels, parent-child relationships,
    and source mappings.

    Args:
        sql: SQL containing CASE statements
        dialect: SQL dialect (snowflake, postgres, tsql, mysql, bigquery)
        name: Optional name for the hierarchy
        session_id: Optional session to associate with

    Returns:
        Dictionary containing:
        - hierarchy_id: Unique hierarchy identifier
        - name: Hierarchy name
        - entity_type: Detected entity type (account, cost_center, etc.)
        - level_count: Number of hierarchy levels
        - node_count: Total nodes in hierarchy
        - root_nodes: Root level nodes
        - confidence: Detection confidence score
        - librarian_compatible: Whether exportable to Librarian format

    Example:
        >>> sql = '''
        ... CASE WHEN account LIKE '4%' THEN 'Revenue'
        ...      WHEN account LIKE '5%' THEN 'COGS'
        ...      WHEN account LIKE '6%' THEN 'OpEx'
        ... END
        ... '''
        >>> result = extract_hierarchy_from_sql(sql)
        >>> print(result["entity_type"])
        "account"
    """
    from databridge_discovery.hierarchy.case_to_hierarchy import CaseToHierarchyConverter
    from databridge_discovery.parser.case_extractor import CaseExtractor

    extractor = CaseExtractor(dialect=dialect)
    cases = extractor.extract_from_sql(sql)

    if not cases:
        return {
            "success": False,
            "error": "No CASE statements found in SQL",
            "sql_preview": sql[:200] + "..." if len(sql) > 200 else sql,
        }

    converter = CaseToHierarchyConverter()
    hierarchies = []

    for case in cases:
        converted = converter.convert(case)
        hierarchy_id = converted.id

        # Store the hierarchy
        _hierarchies[hierarchy_id] = converted

        # Associate with session if provided
        if session_id and session_id in _sessions:
            session = _sessions[session_id]
            session.add_hierarchy(converted)

        hierarchies.append({
            "hierarchy_id": hierarchy_id,
            "name": name or converted.name,
            "entity_type": converted.entity_type,
            "level_count": converted.level_count,
            "node_count": converted.total_nodes,
            "root_nodes": converted.root_nodes,
            "confidence": converted.confidence,
            "source_column": converted.source_column,
            "source_table": converted.source_table,
            "librarian_compatible": True,
        })

    return {
        "success": True,
        "case_count": len(cases),
        "hierarchies": hierarchies,
        "primary_hierarchy": hierarchies[0] if hierarchies else None,
    }


def analyze_csv_for_hierarchy(
    file_path: str | None = None,
    data: list[dict[str, Any]] | None = None,
    target_columns: list[str] | None = None,
    encoding: str = "utf-8",
) -> dict[str, Any]:
    """
    Analyze CSV data to detect potential hierarchy structures.

    Examines column cardinalities, value patterns, and relationships
    to identify columns that may form hierarchies.

    Args:
        file_path: Path to CSV file
        data: Alternative: list of row dictionaries
        target_columns: Specific columns to analyze
        encoding: File encoding

    Returns:
        Dictionary containing:
        - row_count: Number of rows analyzed
        - column_count: Number of columns
        - column_profiles: Profile for each column
        - hierarchy_candidates: Detected hierarchy candidates with:
          - columns: Columns forming the hierarchy
          - confidence: Detection confidence
          - entity_type: Detected entity type
          - level_count: Number of levels
          - sample_path: Sample hierarchy path
        - entity_columns: Columns grouped by entity type
        - relationship_pairs: Parent-child column relationships

    Example:
        >>> result = analyze_csv_for_hierarchy(
        ...     file_path="accounts.csv",
        ...     target_columns=["level1", "level2", "level3"]
        ... )
        >>> print(result["hierarchy_candidates"][0]["level_count"])
        3
    """
    import pandas as pd

    from databridge_discovery.analysis.csv_result_analyzer import CSVResultAnalyzer

    analyzer = CSVResultAnalyzer()

    # Load data
    if file_path:
        analysis = analyzer.analyze_csv_file(file_path, encoding=encoding)
    elif data:
        df = pd.DataFrame(data)
        analysis = analyzer.analyze(df, target_columns=target_columns)
    else:
        return {
            "error": "Either file_path or data must be provided",
        }

    return {
        "row_count": analysis.row_count,
        "column_count": analysis.column_count,
        "column_profiles": {
            col: {
                "data_type": profile.data_type,
                "distinct_count": profile.distinct_count,
                "null_count": profile.null_count,
                "sample_values": profile.sample_values[:5],
                "is_hierarchical": profile.is_hierarchical,
                "detected_entity_type": profile.detected_entity_type.value,
                "hierarchy_confidence": profile.hierarchy_confidence,
                "patterns": profile.patterns,
            }
            for col, profile in analysis.column_profiles.items()
        },
        "hierarchy_candidates": [
            {
                "columns": c.columns,
                "confidence": c.confidence,
                "entity_type": c.entity_type.value,
                "level_count": c.level_count,
                "cardinality_chain": c.cardinality_chain,
                "sample_path": c.sample_path,
                "notes": c.notes,
            }
            for c in analysis.hierarchy_candidates
        ],
        "entity_columns": {
            e.value: cols
            for e, cols in analysis.entity_columns.items()
        },
        "relationship_pairs": [
            {"parent": p, "child": c, "confidence": conf}
            for p, c, conf in analysis.relationship_pairs
        ],
        "notes": analysis.notes,
    }


def detect_entity_types(
    column_name: str | None = None,
    values: list[str] | None = None,
    data: list[dict[str, Any]] | None = None,
    columns: list[str] | None = None,
) -> dict[str, Any]:
    """
    Detect entity types from column names and/or data values.

    Identifies one of 12 standard entity types:
    account, cost_center, department, entity, project, product,
    customer, vendor, employee, location, time_period, currency

    Args:
        column_name: Column name to analyze
        values: Sample values from the column
        data: Full dataset as list of row dicts
        columns: Columns to analyze if data provided

    Returns:
        Dictionary containing:
        - detected_type: Primary detected entity type
        - confidence: Detection confidence (0.0 to 1.0)
        - evidence: Evidence supporting the detection
        - alternative_types: Other possible types
        - column_results: Per-column results (if data provided)

    Example:
        >>> result = detect_entity_types(
        ...     column_name="account_code",
        ...     values=["4100", "4200", "5100", "6100"]
        ... )
        >>> print(result["detected_type"])
        "account"
    """
    import pandas as pd

    from databridge_discovery.analysis.account_detector import AccountDetector

    detector = AccountDetector()

    # Single column detection
    if column_name or values:
        result = detector.detect(column_name=column_name, values=values)

        return {
            "detected_type": result.detected_type.value,
            "confidence": result.confidence,
            "evidence": result.evidence,
            "alternative_types": [
                {"type": t.value, "confidence": c}
                for t, c in result.alternative_types
            ],
            "metadata": result.metadata,
        }

    # DataFrame detection
    if data:
        df = pd.DataFrame(data)
        result = detector.detect_from_dataframe(df, columns=columns)

        return {
            "primary_entity": result.primary_entity.value if result.primary_entity else None,
            "column_results": {
                col: {
                    "detected_type": r.detected_type.value,
                    "confidence": r.confidence,
                    "evidence": r.evidence,
                }
                for col, r in result.column_results.items()
            },
            "entity_coverage": {
                e.value: cols
                for e, cols in result.entity_coverage.items()
            },
            "notes": result.notes,
        }

    return {
        "error": "Must provide column_name/values or data",
        "supported_entity_types": [
            "account", "cost_center", "department", "entity",
            "project", "product", "customer", "vendor",
            "employee", "location", "time_period", "currency",
        ],
    }


def infer_hierarchy_levels(
    values: list[str] | None = None,
    data: list[dict[str, Any]] | None = None,
    column: str | None = None,
    detect_method: str = "auto",
) -> dict[str, Any]:
    """
    Infer hierarchy level structure from data.

    Analyzes codes, patterns, or delimited values to determine
    hierarchy levels and their relationships.

    Args:
        values: List of values to analyze
        data: DataFrame as list of row dicts
        column: Column to analyze if data provided
        detect_method: Detection method (auto, prefix, delimiter, cardinality)

    Returns:
        Dictionary containing:
        - level_count: Number of levels detected
        - levels: Level details with:
          - level_number: 1-based level
          - level_name: Suggested name
          - value_count: Distinct values
          - sample_values: Sample values
          - pattern: Detected pattern
        - confidence: Detection confidence
        - detection_method: Method used
        - notes: Detection notes

    Example:
        >>> result = infer_hierarchy_levels(
        ...     values=["1-000", "1-100", "1-110", "1-111"]
        ... )
        >>> print(result["level_count"])
        3
    """
    import pandas as pd

    from databridge_discovery.hierarchy.level_detector import LevelDetector

    detector = LevelDetector()

    if data and column:
        df = pd.DataFrame(data)
        result = detector.detect_from_dataframe(df, column)
    elif values:
        # Try different detection methods
        if detect_method == "prefix" or detect_method == "auto":
            result = detector.detect_from_code_prefixes(values)
            if result.confidence < 0.5 and detect_method == "auto":
                # Try pattern-based
                result = detector.detect_from_patterns(values)
        else:
            result = detector.detect_from_patterns(values)
    else:
        return {
            "error": "Must provide values or data/column",
        }

    return {
        "level_count": result.total_levels,
        "levels": [
            {
                "level_number": l.level_number,
                "level_name": l.level_name,
                "value_count": l.value_count,
                "sample_values": l.distinct_values[:10] if l.distinct_values else [],
                "pattern": l.pattern,
            }
            for l in result.levels
        ],
        "confidence": result.confidence,
        "detection_method": result.detection_method,
        "notes": result.notes,
    }


def generate_sort_orders(
    values: list[str],
    entity_type: str | None = None,
    method: str = "auto",
    custom_order: dict[str, int] | None = None,
) -> dict[str, Any]:
    """
    Generate sort orders for hierarchy values.

    Infers appropriate sort orders based on financial conventions,
    numeric prefixes, or custom ordering.

    Args:
        values: Values to sort
        entity_type: Entity type hint (account, product, etc.)
        method: Sort method (auto, financial, numeric, alphabetical, custom)
        custom_order: Custom sort order map

    Returns:
        Dictionary containing:
        - sorted_values: Values in sorted order
        - sort_orders: Value to sort order mapping
        - method_used: Actual method used
        - confidence: Sort confidence
        - notes: Sorting notes

    Example:
        >>> result = generate_sort_orders(
        ...     values=["COGS", "Revenue", "OpEx", "Gross Profit"],
        ...     entity_type="account"
        ... )
        >>> print(result["sorted_values"])
        ["Revenue", "COGS", "Gross Profit", "OpEx"]
    """
    from databridge_discovery.hierarchy.sort_order_inferrer import (
        SortMethod,
        SortOrderInferrer,
    )

    inferrer = SortOrderInferrer()

    if method == "custom" and custom_order:
        result = inferrer.apply_custom_order(values, custom_order)
    elif method == "financial":
        result = inferrer._try_financial_sort(values)
    elif method == "numeric":
        result = inferrer._try_numeric_prefix_sort(values)
        if result.confidence < 0.5:
            result = inferrer._try_numeric_sort(values)
    elif method == "alphabetical":
        result = inferrer._try_alphabetical_sort(values)
    else:
        # Auto detection
        result = inferrer.infer_sort_order(values, entity_type)

    return {
        "sorted_values": result.values,
        "sort_orders": result.sort_orders,
        "method_used": result.method.value,
        "confidence": result.confidence,
        "notes": result.notes,
    }


def merge_with_librarian_hierarchy(
    source_hierarchy_id: str,
    librarian_hierarchy_csv: list[dict[str, Any]],
    librarian_mapping_csv: list[dict[str, Any]] | None = None,
    strategy: str = "merge_both",
    resolve_conflicts: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Merge a discovered hierarchy with an existing Librarian hierarchy.

    Combines source and target hierarchies, detecting conflicts
    and applying merge strategies.

    Args:
        source_hierarchy_id: ID of discovered hierarchy to merge
        librarian_hierarchy_csv: Librarian HIERARCHY.CSV data as list of row dicts
        librarian_mapping_csv: Librarian MAPPING.CSV data (optional)
        strategy: Merge strategy (keep_existing, prefer_new, merge_both, interactive)
        resolve_conflicts: Pre-resolved conflicts (value -> resolution)

    Returns:
        Dictionary containing:
        - success: Whether merge succeeded
        - merged_hierarchy_id: ID of merged result
        - additions: Values added from source
        - updates: Values updated in target
        - conflicts: Unresolved conflicts
        - notes: Merge notes

    Example:
        >>> result = merge_with_librarian_hierarchy(
        ...     source_hierarchy_id="hier_abc123",
        ...     librarian_hierarchy_csv=hierarchy_rows,
        ...     strategy="prefer_new"
        ... )
        >>> print(result["additions"])
        ["New Category 1", "New Category 2"]
    """
    from databridge_discovery.hierarchy.hierarchy_merger import (
        HierarchyMerger,
        MergeStrategy,
    )

    if source_hierarchy_id not in _hierarchies:
        return {
            "error": f"Source hierarchy not found: {source_hierarchy_id}",
            "available_hierarchies": list(_hierarchies.keys()),
        }

    source = _hierarchies[source_hierarchy_id]

    # Parse strategy
    try:
        strategy_enum = MergeStrategy(strategy)
    except ValueError:
        return {
            "error": f"Invalid strategy: {strategy}",
            "valid_strategies": [s.value for s in MergeStrategy],
        }

    merger = HierarchyMerger(strategy=strategy_enum)
    result = merger.merge_from_librarian_csv(
        source=source,
        hierarchy_rows=librarian_hierarchy_csv,
        mapping_rows=librarian_mapping_csv,
    )

    # Store merged hierarchy if successful
    if result.success and result.merged_hierarchy:
        merged_id = result.merged_hierarchy.id
        _hierarchies[merged_id] = result.merged_hierarchy
    else:
        merged_id = None

    return {
        "success": result.success,
        "merged_hierarchy_id": merged_id,
        "additions": result.additions,
        "updates": result.updates,
        "deletions": result.deletions,
        "conflicts": [
            {
                "type": c.conflict_type.value,
                "value": c.value,
                "source_info": c.source_info,
                "target_info": c.target_info,
                "resolved": c.resolved,
                "resolution": c.resolution,
            }
            for c in result.conflicts
        ],
        "notes": result.notes,
    }


def export_discovery_as_csv(
    hierarchy_id: str,
    output_dir: str,
    file_prefix: str = "DISCOVERED",
) -> dict[str, Any]:
    """
    Export discovered hierarchy to Librarian-compatible CSV files.

    Creates HIERARCHY.CSV and MAPPING.CSV files in Librarian format
    for import into the hierarchy management system.

    Args:
        hierarchy_id: ID of hierarchy to export
        output_dir: Directory to write CSV files
        file_prefix: Prefix for file names

    Returns:
        Dictionary containing:
        - success: Whether export succeeded
        - hierarchy_file: Path to HIERARCHY.CSV
        - mapping_file: Path to MAPPING.CSV
        - row_counts: Row counts for each file
        - notes: Export notes

    Example:
        >>> result = export_discovery_as_csv(
        ...     hierarchy_id="hier_abc123",
        ...     output_dir="./exports",
        ...     file_prefix="GL_HIERARCHY"
        ... )
        >>> print(result["hierarchy_file"])
        "./exports/GL_HIERARCHY_HIERARCHY.CSV"
    """
    import csv
    from pathlib import Path

    from databridge_discovery.hierarchy.case_to_hierarchy import CaseToHierarchyConverter

    if hierarchy_id not in _hierarchies:
        return {
            "error": f"Hierarchy not found: {hierarchy_id}",
            "available_hierarchies": list(_hierarchies.keys()),
        }

    hierarchy = _hierarchies[hierarchy_id]
    converter = CaseToHierarchyConverter()

    # Generate Librarian CSV data
    hierarchy_rows = converter.to_librarian_hierarchy_csv(hierarchy)
    mapping_rows = converter.to_librarian_mapping_csv(hierarchy)

    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Write hierarchy CSV
        hier_file = output_path / f"{file_prefix}_HIERARCHY.CSV"
        if hierarchy_rows:
            with open(hier_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=hierarchy_rows[0].keys())
                writer.writeheader()
                writer.writerows(hierarchy_rows)

        # Write mapping CSV
        map_file = output_path / f"{file_prefix}_HIERARCHY_MAPPING.CSV"
        if mapping_rows:
            with open(map_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=mapping_rows[0].keys())
                writer.writeheader()
                writer.writerows(mapping_rows)

        return {
            "success": True,
            "hierarchy_file": str(hier_file.absolute()),
            "mapping_file": str(map_file.absolute()),
            "row_counts": {
                "hierarchy": len(hierarchy_rows),
                "mapping": len(mapping_rows),
            },
            "notes": [
                f"Exported {len(hierarchy_rows)} hierarchy rows",
                f"Exported {len(mapping_rows)} mapping rows",
            ],
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


def validate_hierarchy_structure(
    hierarchy_id: str,
    check_orphans: bool = True,
    check_cycles: bool = True,
    check_levels: bool = True,
) -> dict[str, Any]:
    """
    Validate hierarchy structure for integrity issues.

    Checks for common problems like orphan nodes, circular references,
    level inconsistencies, and missing parents.

    Args:
        hierarchy_id: ID of hierarchy to validate
        check_orphans: Check for orphan nodes (children without parents)
        check_cycles: Check for circular parent references
        check_levels: Check for level consistency

    Returns:
        Dictionary containing:
        - valid: Whether hierarchy passes all checks
        - errors: Critical issues that must be fixed
        - warnings: Non-critical issues
        - stats: Validation statistics

    Example:
        >>> result = validate_hierarchy_structure("hier_abc123")
        >>> print(result["valid"])
        True
    """
    if hierarchy_id not in _hierarchies:
        return {
            "error": f"Hierarchy not found: {hierarchy_id}",
            "available_hierarchies": list(_hierarchies.keys()),
        }

    hierarchy = _hierarchies[hierarchy_id]
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    # Check for orphan nodes
    if check_orphans:
        for node_id, node in hierarchy.nodes.items():
            if node.parent_id and node.parent_id not in hierarchy.nodes:
                errors.append({
                    "type": "orphan_node",
                    "node_id": node_id,
                    "node_name": node.name,
                    "missing_parent_id": node.parent_id,
                    "message": f"Node '{node.name}' references non-existent parent '{node.parent_id}'",
                })

    # Check for cycles
    if check_cycles:
        visited: set[str] = set()

        def has_cycle(node_id: str, path: list[str]) -> bool:
            if node_id in path:
                return True
            if node_id in visited:
                return False

            visited.add(node_id)
            node = hierarchy.nodes.get(node_id)
            if node and node.parent_id:
                return has_cycle(node.parent_id, path + [node_id])
            return False

        for node_id in hierarchy.nodes:
            if has_cycle(node_id, []):
                errors.append({
                    "type": "circular_reference",
                    "node_id": node_id,
                    "message": f"Circular reference detected starting from node '{node_id}'",
                })
                break  # One cycle error is enough

    # Check level consistency
    if check_levels:
        for node_id, node in hierarchy.nodes.items():
            if node.parent_id:
                parent = hierarchy.nodes.get(node.parent_id)
                if parent and node.level <= parent.level:
                    warnings.append({
                        "type": "level_inconsistency",
                        "node_id": node_id,
                        "node_level": node.level,
                        "parent_level": parent.level,
                        "message": f"Node '{node.name}' (level {node.level}) should be deeper than parent (level {parent.level})",
                    })

    # Check for empty nodes
    empty_nodes = [
        n for n in hierarchy.nodes.values()
        if not n.name or not n.value
    ]
    if empty_nodes:
        for node in empty_nodes:
            warnings.append({
                "type": "empty_node",
                "node_id": node.id,
                "message": f"Node '{node.id}' has empty name or value",
            })

    return {
        "valid": len(errors) == 0,
        "hierarchy_id": hierarchy_id,
        "errors": errors,
        "warnings": warnings,
        "stats": {
            "total_nodes": len(hierarchy.nodes),
            "root_nodes": len(hierarchy.root_nodes),
            "max_level": hierarchy.level_count,
            "error_count": len(errors),
            "warning_count": len(warnings),
        },
    }


def suggest_parent_child_relationships(
    hierarchy_id: str | None = None,
    values: list[str] | None = None,
    similarity_threshold: float = 0.8,
) -> dict[str, Any]:
    """
    Suggest parent-child relationships between hierarchy values.

    Analyzes values to find potential parent-child groupings
    based on naming patterns, prefixes, and semantic similarity.

    Args:
        hierarchy_id: Existing hierarchy to analyze
        values: List of values to analyze
        similarity_threshold: Minimum similarity for suggestions

    Returns:
        Dictionary containing:
        - suggestions: List of suggested relationships with:
          - parent: Suggested parent value
          - child: Suggested child value
          - confidence: Relationship confidence
          - reason: Why this relationship was suggested
        - potential_roots: Suggested root nodes
        - notes: Analysis notes

    Example:
        >>> result = suggest_parent_child_relationships(
        ...     values=["Revenue", "Product Revenue", "Service Revenue"]
        ... )
        >>> print(result["suggestions"][0]["parent"])
        "Revenue"
    """
    suggestions: list[dict[str, Any]] = []
    potential_roots: list[str] = []

    if hierarchy_id and hierarchy_id in _hierarchies:
        hierarchy = _hierarchies[hierarchy_id]
        values = [n.value for n in hierarchy.nodes.values()]
    elif not values:
        return {
            "error": "Must provide hierarchy_id or values",
        }

    # Analyze for prefix patterns
    for potential_parent in values:
        for potential_child in values:
            if potential_parent == potential_child:
                continue

            parent_lower = potential_parent.lower()
            child_lower = potential_child.lower()

            # Child starts with parent
            if child_lower.startswith(parent_lower) and len(child_lower) > len(parent_lower):
                suggestions.append({
                    "parent": potential_parent,
                    "child": potential_child,
                    "confidence": 0.9,
                    "reason": "child_starts_with_parent",
                })
            # Parent is word subset of child
            elif parent_lower in child_lower:
                suggestions.append({
                    "parent": potential_parent,
                    "child": potential_child,
                    "confidence": 0.7,
                    "reason": "parent_is_substring",
                })

    # Find potential roots (values not suggested as children)
    child_values = {s["child"] for s in suggestions}
    potential_roots = [v for v in values if v not in child_values]

    # Use fuzzy matching for additional suggestions
    try:
        from rapidfuzz import fuzz

        for val1 in values:
            for val2 in values:
                if val1 == val2:
                    continue

                # Check if already suggested
                already_suggested = any(
                    s["parent"] == val1 and s["child"] == val2
                    for s in suggestions
                )
                if already_suggested:
                    continue

                # Check semantic similarity
                score = fuzz.ratio(val1.lower(), val2.lower()) / 100.0
                if score >= similarity_threshold and len(val1) < len(val2):
                    suggestions.append({
                        "parent": val1,
                        "child": val2,
                        "confidence": score * 0.6,
                        "reason": "semantic_similarity",
                    })

    except ImportError:
        pass  # RapidFuzz not available

    # Sort by confidence
    suggestions.sort(key=lambda s: s["confidence"], reverse=True)

    return {
        "suggestions": suggestions[:50],  # Limit results
        "potential_roots": potential_roots,
        "total_suggestions": len(suggestions),
        "notes": [
            f"Found {len(suggestions)} potential relationships",
            f"Identified {len(potential_roots)} potential root nodes",
        ],
    }


def compare_hierarchies(
    hierarchy1_id: str,
    hierarchy2_id: str,
    ignore_case: bool = True,
) -> dict[str, Any]:
    """
    Compare two hierarchies and show differences.

    Performs a detailed comparison of two hierarchies, showing
    values unique to each, structural differences, and common values.

    Args:
        hierarchy1_id: ID of first hierarchy
        hierarchy2_id: ID of second hierarchy
        ignore_case: Ignore case when comparing values

    Returns:
        Dictionary containing:
        - are_equal: Whether hierarchies are identical
        - only_in_first: Values only in first hierarchy
        - only_in_second: Values only in second hierarchy
        - common_values: Values in both hierarchies
        - structural_differences: Differences in structure
        - summary: Comparison summary

    Example:
        >>> result = compare_hierarchies("hier1", "hier2")
        >>> print(result["only_in_first"])
        ["Deprecated Category", "Old Account"]
    """
    from databridge_discovery.hierarchy.hierarchy_merger import HierarchyMerger

    if hierarchy1_id not in _hierarchies:
        return {
            "error": f"First hierarchy not found: {hierarchy1_id}",
            "available_hierarchies": list(_hierarchies.keys()),
        }

    if hierarchy2_id not in _hierarchies:
        return {
            "error": f"Second hierarchy not found: {hierarchy2_id}",
            "available_hierarchies": list(_hierarchies.keys()),
        }

    hier1 = _hierarchies[hierarchy1_id]
    hier2 = _hierarchies[hierarchy2_id]

    merger = HierarchyMerger(ignore_case=ignore_case)
    comparison = merger.compare(hier1, hier2)

    return {
        "are_equal": comparison.are_equal,
        "hierarchy1_id": hierarchy1_id,
        "hierarchy2_id": hierarchy2_id,
        "only_in_first": comparison.source_only,
        "only_in_second": comparison.target_only,
        "common_values": comparison.common_values,
        "structural_differences": comparison.structural_differences,
        "other_differences": comparison.differences,
        "summary": {
            "only_in_first_count": len(comparison.source_only),
            "only_in_second_count": len(comparison.target_only),
            "common_count": len(comparison.common_values),
            "structural_diff_count": len(comparison.structural_differences),
        },
        "notes": comparison.notes,
    }


# Hierarchy utility functions

def list_hierarchies() -> dict[str, Any]:
    """
    List all available hierarchies.

    Returns:
        Dictionary with hierarchy list
    """
    return {
        "hierarchies": [
            {
                "hierarchy_id": hid,
                "name": h.name,
                "entity_type": h.entity_type,
                "node_count": h.total_nodes,
                "level_count": h.level_count,
            }
            for hid, h in _hierarchies.items()
        ],
        "total_count": len(_hierarchies),
    }


def get_hierarchy_details(hierarchy_id: str) -> dict[str, Any]:
    """
    Get detailed information about a hierarchy.

    Args:
        hierarchy_id: Hierarchy identifier

    Returns:
        Dictionary with full hierarchy details
    """
    if hierarchy_id not in _hierarchies:
        return {"error": f"Hierarchy not found: {hierarchy_id}"}

    hierarchy = _hierarchies[hierarchy_id]

    return {
        "hierarchy_id": hierarchy_id,
        "name": hierarchy.name,
        "entity_type": hierarchy.entity_type,
        "level_count": hierarchy.level_count,
        "total_nodes": hierarchy.total_nodes,
        "root_nodes": hierarchy.root_nodes,
        "source_column": hierarchy.source_column,
        "source_table": hierarchy.source_table,
        "confidence": hierarchy.confidence,
        "nodes": {
            nid: {
                "name": n.name,
                "value": n.value,
                "level": n.level,
                "parent_id": n.parent_id,
                "children": n.children,
                "sort_order": n.sort_order,
            }
            for nid, n in hierarchy.nodes.items()
        },
        "mapping": hierarchy.mapping,
    }


def delete_hierarchy(hierarchy_id: str) -> dict[str, Any]:
    """
    Delete a hierarchy.

    Args:
        hierarchy_id: Hierarchy identifier

    Returns:
        Dictionary with deletion status
    """
    if hierarchy_id not in _hierarchies:
        return {"error": f"Hierarchy not found: {hierarchy_id}"}

    del _hierarchies[hierarchy_id]

    return {
        "success": True,
        "deleted": hierarchy_id,
    }


# =============================================================================
# Phase 4 - Project Generation & Documentation (8 tools)
# =============================================================================


# Global project storage
_generated_projects: dict[str, Any] = {}


def generate_librarian_project(
    hierarchy_ids: list[str] | None = None,
    project_name: str = "DISCOVERED_PROJECT",
    output_format: str = "snowflake",
    target_schema: str = "HIERARCHIES",
    include_tiers: list[str] | None = None,
    generate_dbt: bool = False,
    output_dir: str | None = None,
) -> dict[str, Any]:
    """
    Generate a complete Librarian project from discovered hierarchies.

    Creates a full project structure with:
    - TBL_0 hierarchy tables
    - VW_1 mapping views
    - Optional DT_2, DT_3A, DT_3 tables
    - Deployment scripts
    - Documentation

    Args:
        hierarchy_ids: List of hierarchy IDs to include (default: all)
        project_name: Name for the generated project
        output_format: SQL dialect (snowflake, postgresql, bigquery, tsql)
        target_schema: Target schema name
        include_tiers: Tiers to generate (TBL_0, VW_1, DT_2, DT_3A, DT_3)
        generate_dbt: Whether to generate dbt models
        output_dir: Optional directory to write files

    Returns:
        Dictionary with project details and generated files
    """
    from databridge_discovery.generation.project_generator import (
        ProjectGenerator,
        ProjectConfig,
        OutputFormat,
        ProjectTier,
    )

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found to generate project from"}

    # Parse output format
    try:
        format_enum = OutputFormat(output_format.lower())
    except ValueError:
        format_enum = OutputFormat.SNOWFLAKE

    # Parse tiers
    tier_enums = []
    if include_tiers:
        for tier in include_tiers:
            try:
                tier_enums.append(ProjectTier(tier.upper()))
            except ValueError:
                pass
    else:
        tier_enums = [ProjectTier.TBL_0, ProjectTier.VW_1]

    # Create config
    config = ProjectConfig(
        project_name=project_name,
        output_format=format_enum,
        target_schema=target_schema,
        include_tiers=tier_enums,
        generate_dbt=generate_dbt,
    )

    # Generate project
    generator = ProjectGenerator(dialect=output_format)
    project = generator.generate(hierarchies, config)

    # Store project
    import uuid
    project_id = str(uuid.uuid4())[:8]
    _generated_projects[project_id] = project

    # Write to disk if output_dir provided
    files_written = {}
    if output_dir:
        files_written = generator.write_project(project, output_dir)

    return {
        "project_id": project_id,
        "project_name": project.project_name,
        "hierarchies_included": project.hierarchies,
        "tiers_generated": [t.value for t in project.tiers_generated],
        "file_count": project.file_count,
        "files": [
            {
                "name": f.name,
                "path": f.path,
                "type": f.file_type,
                "tier": f.tier.value if f.tier else None,
            }
            for f in project.files
        ],
        "output_dir": output_dir,
        "files_written": files_written,
        "notes": project.notes,
    }


def generate_hierarchy_from_discovery(
    session_id: str,
    hierarchy_proposal_id: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """
    Create a hierarchy from a discovery session proposal.

    Takes a proposed hierarchy from analysis and converts it
    to a full ConvertedHierarchy for project generation.

    Args:
        session_id: Discovery session ID
        hierarchy_proposal_id: Optional proposal ID to convert
        name: Optional name override

    Returns:
        Dictionary with created hierarchy details
    """
    from databridge_discovery.hierarchy.case_to_hierarchy import CaseToHierarchyConverter

    if session_id not in _sessions:
        return {"error": f"Session not found: {session_id}"}

    session = _sessions[session_id]

    # Get proposals
    proposals = session.get("proposals", [])
    if not proposals:
        return {"error": "No hierarchy proposals in session"}

    # Select proposal
    proposal = None
    if hierarchy_proposal_id:
        for p in proposals:
            if p.get("id") == hierarchy_proposal_id:
                proposal = p
                break
        if not proposal:
            return {"error": f"Proposal not found: {hierarchy_proposal_id}"}
    else:
        # Use first approved or first proposal
        approved = [p for p in proposals if p.get("status") == "approved"]
        proposal = approved[0] if approved else proposals[0]

    # Convert proposal to hierarchy
    converter = CaseToHierarchyConverter()

    # Get CASE statements from session
    case_statements = session.get("case_statements", [])
    if not case_statements:
        return {"error": "No CASE statements found in session"}

    # Use first case statement for now
    from databridge_discovery.parser.case_extractor import ExtractedCase
    case_data = case_statements[0]

    # Build ExtractedCase from stored data
    case = ExtractedCase(
        alias=case_data.get("alias", "hierarchy"),
        when_clauses=case_data.get("when_clauses", []),
        else_clause=case_data.get("else_clause"),
        source_column=case_data.get("source_column"),
        source_table=case_data.get("source_table"),
    )

    # Convert
    hierarchy = converter.convert(case, name=name or proposal.get("name", "discovered_hierarchy"))

    # Store hierarchy
    import uuid
    hierarchy_id = hierarchy.id or str(uuid.uuid4())[:8]
    _hierarchies[hierarchy_id] = hierarchy

    return {
        "success": True,
        "hierarchy_id": hierarchy_id,
        "name": hierarchy.name,
        "entity_type": hierarchy.entity_type,
        "level_count": hierarchy.level_count,
        "total_nodes": hierarchy.total_nodes,
        "source_column": hierarchy.source_column,
        "confidence": hierarchy.confidence,
    }


def generate_vw1_views(
    hierarchy_ids: list[str] | None = None,
    dialect: str = "snowflake",
    target_schema: str = "HIERARCHIES",
    view_types: list[str] | None = None,
) -> dict[str, Any]:
    """
    Generate VW_1 tier views for hierarchies.

    Creates various view types:
    - mapping: Standard mapping view
    - unnest: Unnested array view
    - filtered: Filtered active-only view
    - rollup: Aggregation rollup view
    - precedence: Precedence-based view

    Args:
        hierarchy_ids: List of hierarchy IDs (default: all)
        dialect: SQL dialect (snowflake, postgresql, bigquery)
        target_schema: Target schema name
        view_types: Types of views to generate

    Returns:
        Dictionary with generated view DDL
    """
    from databridge_discovery.generation.view_generator import (
        ViewGenerator,
        ViewConfig,
        ViewDialect,
        ViewType,
    )

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found"}

    # Parse dialect
    try:
        dialect_enum = ViewDialect(dialect.lower())
    except ValueError:
        dialect_enum = ViewDialect.SNOWFLAKE

    # Parse view types
    type_enums = []
    if view_types:
        for vt in view_types:
            try:
                type_enums.append(ViewType(f"vw_1_{vt.lower()}"))
            except ValueError:
                pass
    if not type_enums:
        type_enums = [ViewType.VW_1_MAPPING]

    # Create generator
    generator = ViewGenerator(dialect=dialect_enum)
    config = ViewConfig(target_schema=target_schema, dialect=dialect_enum)

    # Generate views
    all_views = []
    for hier in hierarchies:
        views = generator.generate_all_views(hier, config, type_enums)
        all_views.extend(views)

    return {
        "view_count": len(all_views),
        "dialect": dialect,
        "views": [
            {
                "name": v.name,
                "view_type": v.view_type.value,
                "source_table": v.source_table,
                "columns": v.columns,
                "ddl": v.ddl,
            }
            for v in all_views
        ],
    }


def generate_dbt_models(
    hierarchy_ids: list[str] | None = None,
    project_name: str = "hierarchy_analytics",
    source_database: str = "RAW",
    source_schema: str = "HIERARCHIES",
    target_schema: str = "ANALYTICS",
    include_staging: bool = True,
    include_intermediate: bool = True,
    include_marts: bool = True,
    output_dir: str | None = None,
) -> dict[str, Any]:
    """
    Generate dbt models from discovered hierarchies.

    Creates a complete dbt project structure:
    - dbt_project.yml
    - sources.yml
    - schema.yml
    - Staging models (stg_*)
    - Intermediate models (int_*)
    - Mart models (dim_*)

    Args:
        hierarchy_ids: List of hierarchy IDs (default: all)
        project_name: dbt project name
        source_database: Source database name
        source_schema: Source schema name
        target_schema: Target schema name
        include_staging: Generate staging models
        include_intermediate: Generate intermediate models
        include_marts: Generate mart models
        output_dir: Optional directory to write files

    Returns:
        Dictionary with dbt project details
    """
    from databridge_discovery.generation.dbt_generator import (
        DbtGenerator,
        DbtGeneratorConfig,
    )

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found"}

    # Create config
    config = DbtGeneratorConfig(
        project_name=project_name,
        source_database=source_database,
        source_schema=source_schema,
        target_schema=target_schema,
        include_staging=include_staging,
        include_intermediate=include_intermediate,
        include_marts=include_marts,
    )

    # Generate project
    generator = DbtGenerator()
    project = generator.generate_project(hierarchies, config)

    # Write to disk if output_dir provided
    files_written = {}
    if output_dir:
        files_written = generator.write_project(project, output_dir)

    return {
        "project_name": project.name,
        "model_count": project.model_count,
        "sources": [
            {
                "name": s.name,
                "database": s.database,
                "schema": s.schema,
                "table_count": len(s.tables),
            }
            for s in project.sources
        ],
        "models": [
            {
                "name": m.name,
                "layer": m.layer.value,
                "materialization": m.materialization.value,
                "file_path": m.file_path,
            }
            for m in project.models
        ],
        "output_dir": output_dir,
        "files_written": files_written,
    }


def generate_data_dictionary(
    hierarchy_ids: list[str] | None = None,
    name: str = "Hierarchy Data Dictionary",
    output_format: str = "markdown",
    output_path: str | None = None,
) -> dict[str, Any]:
    """
    Generate a data dictionary for discovered hierarchies.

    Creates comprehensive documentation including:
    - Table definitions
    - Column definitions with types and descriptions
    - Business names and examples
    - Export to markdown, CSV, or JSON

    Args:
        hierarchy_ids: List of hierarchy IDs (default: all)
        name: Dictionary name
        output_format: Format (markdown, csv, json)
        output_path: Optional path to write output

    Returns:
        Dictionary with data dictionary content
    """
    from databridge_discovery.documentation.data_dictionary import DataDictionaryGenerator

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found"}

    # Generate dictionary
    generator = DataDictionaryGenerator()
    dictionary = generator.generate(hierarchies, name)

    # Export to requested format
    content = ""
    if output_format.lower() == "markdown":
        content = generator.to_markdown(dictionary)
    elif output_format.lower() == "csv":
        content = generator.to_csv(dictionary)
    elif output_format.lower() == "json":
        import json
        content = json.dumps(dictionary.to_dict(), indent=2)
    else:
        content = generator.to_markdown(dictionary)

    # Write to file if path provided
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return {
        "name": dictionary.name,
        "table_count": dictionary.table_count,
        "total_columns": dictionary.total_columns,
        "format": output_format,
        "output_path": output_path,
        "content": content if len(content) < 10000 else content[:10000] + "\n... (truncated)",
        "tables": [
            {
                "name": t.name,
                "column_count": t.column_count,
                "description": t.description,
            }
            for t in dictionary.tables
        ],
    }


def export_lineage_diagram(
    hierarchy_ids: list[str] | None = None,
    name: str = "Hierarchy Lineage",
    output_format: str = "mermaid",
    output_path: str | None = None,
) -> dict[str, Any]:
    """
    Export lineage diagram for discovered hierarchies.

    Generates lineage visualization in multiple formats:
    - mermaid: Mermaid flowchart (for GitHub/GitLab)
    - d2: D2 diagram format
    - graphviz: GraphViz DOT format
    - html: Interactive HTML with Mermaid
    - markdown: Markdown with embedded diagram

    Args:
        hierarchy_ids: List of hierarchy IDs (default: all)
        name: Diagram name
        output_format: Format (mermaid, d2, graphviz, html, markdown)
        output_path: Optional path to write output

    Returns:
        Dictionary with diagram content
    """
    from databridge_discovery.documentation.lineage_documenter import LineageDocumenter

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found"}

    # Build lineage
    documenter = LineageDocumenter()
    diagram = documenter.build_lineage(hierarchies, name)

    # Export to requested format
    content = ""
    format_lower = output_format.lower()
    if format_lower == "mermaid":
        content = documenter.to_mermaid(diagram)
    elif format_lower == "d2":
        content = documenter.to_d2(diagram)
    elif format_lower == "graphviz" or format_lower == "dot":
        content = documenter.to_graphviz(diagram)
    elif format_lower == "html":
        content = documenter.to_html(diagram)
    elif format_lower == "markdown":
        content = documenter.to_markdown(diagram)
    else:
        content = documenter.to_mermaid(diagram)

    # Write to file if path provided
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

    return {
        "name": diagram.name,
        "node_count": diagram.node_count,
        "edge_count": diagram.edge_count,
        "format": output_format,
        "output_path": output_path,
        "content": content if len(content) < 10000 else content[:10000] + "\n... (truncated)",
    }


def validate_generated_project(
    project_id: str,
) -> dict[str, Any]:
    """
    Validate a generated project for completeness.

    Checks for:
    - Required files present
    - SQL syntax validity (basic)
    - Tier completeness
    - Deployment script

    Args:
        project_id: Project ID from generate_librarian_project

    Returns:
        Dictionary with validation results
    """
    from databridge_discovery.generation.project_generator import ProjectGenerator

    if project_id not in _generated_projects:
        return {"error": f"Project not found: {project_id}"}

    project = _generated_projects[project_id]

    # Validate
    generator = ProjectGenerator()
    validation = generator.validate_project(project)

    return {
        "project_id": project_id,
        "project_name": project.project_name,
        "valid": validation["valid"],
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "file_count": validation["file_count"],
        "tiers": validation["tiers"],
    }


def preview_deployment_scripts(
    hierarchy_ids: list[str] | None = None,
    dialect: str = "snowflake",
    target_schema: str = "HIERARCHIES",
    include_drop: bool = True,
) -> dict[str, Any]:
    """
    Preview deployment DDL scripts without writing files.

    Generates complete deployment scripts for:
    - Schema creation
    - Table creation
    - View creation
    - Data insertion

    Args:
        hierarchy_ids: List of hierarchy IDs (default: all)
        dialect: SQL dialect (snowflake, postgresql, bigquery, tsql)
        target_schema: Target schema name
        include_drop: Include DROP statements

    Returns:
        Dictionary with deployment script content
    """
    from databridge_discovery.generation.sql_generator import (
        SQLGenerator,
        SQLGeneratorConfig,
        SQLDialect,
    )

    # Get hierarchies
    hierarchies = []
    if hierarchy_ids:
        for hid in hierarchy_ids:
            if hid in _hierarchies:
                hierarchies.append(_hierarchies[hid])
    else:
        hierarchies = list(_hierarchies.values())

    if not hierarchies:
        return {"error": "No hierarchies found"}

    # Parse dialect
    try:
        dialect_enum = SQLDialect(dialect.lower())
    except ValueError:
        dialect_enum = SQLDialect.SNOWFLAKE

    # Create config
    config = SQLGeneratorConfig(
        dialect=dialect_enum,
        target_schema=target_schema,
        include_drop=include_drop,
    )

    # Generate deployment script
    generator = SQLGenerator(dialect=dialect_enum)
    script = generator.generate_deployment_script(hierarchies, config)

    # Get individual DDLs
    ddls = []
    for hier in hierarchies:
        table_ddl = generator.generate_table_ddl(hier, config)
        view_ddl = generator.generate_view_ddl(hier, config)
        ddls.append({
            "hierarchy": hier.name,
            "table_ddl": generator.to_dict(table_ddl),
            "view_ddl": generator.to_dict(view_ddl),
        })

    return {
        "dialect": dialect,
        "target_schema": target_schema,
        "hierarchy_count": len(hierarchies),
        "script_length": len(script),
        "script": script if len(script) < 20000 else script[:20000] + "\n-- ... (truncated)",
        "ddls": ddls,
    }


# =============================================================================
# Phase 5: Multi-Agent Orchestration Tools (12 tools)
# =============================================================================


def _get_or_create_orchestrator(orchestrator_id: str | None = None) -> tuple[str, Any]:
    """Get existing or create new orchestrator."""
    from databridge_discovery.agents.orchestrator import Orchestrator
    from databridge_discovery.agents.schema_scanner import SchemaScanner
    from databridge_discovery.agents.logic_extractor import LogicExtractor
    from databridge_discovery.agents.warehouse_architect import WarehouseArchitect
    from databridge_discovery.agents.deploy_validator import DeployValidator

    if orchestrator_id and orchestrator_id in _orchestrators:
        return orchestrator_id, _orchestrators[orchestrator_id]

    # Create new orchestrator with default agents
    orchestrator = Orchestrator()
    orchestrator.register_agent("scanner", SchemaScanner())
    orchestrator.register_agent("extractor", LogicExtractor())
    orchestrator.register_agent("architect", WarehouseArchitect())
    orchestrator.register_agent("validator", DeployValidator())

    orch_id = orchestrator.id
    _orchestrators[orch_id] = orchestrator
    return orch_id, orchestrator


def start_discovery_workflow(
    tables: list[dict[str, Any]] | None = None,
    sql: str | None = None,
    schema_name: str = "SOURCE",
    target_schema: str = "ANALYTICS",
    scan_schema: bool = True,
    extract_logic: bool = True,
    design_model: bool = True,
    generate_dbt: bool = True,
    validate_deployment: bool = False,
    dry_run: bool = True,
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Start an end-to-end discovery workflow.

    Orchestrates the full discovery process through 5 phases:
    1. Scan - Extract schema metadata from tables
    2. Extract - Parse SQL and extract CASE statements
    3. Design - Create star schema design
    4. Generate - Generate dbt models
    5. Validate - Validate deployment (optional)

    Args:
        tables: List of table metadata dictionaries
        sql: SQL query with CASE statements to analyze
        schema_name: Source schema name
        target_schema: Target schema for generated models
        scan_schema: Run schema scanning phase
        extract_logic: Run SQL logic extraction phase
        design_model: Run star schema design phase
        generate_dbt: Generate dbt models
        validate_deployment: Run deployment validation
        dry_run: Preview mode (no actual deployment)
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - workflow_id: Unique workflow ID
        - execution_id: Execution instance ID
        - status: Current status (pending, running, completed, failed)
        - phases_completed: List of completed phases
        - hierarchy_count: Number of hierarchies discovered
        - model_count: Number of models generated
        - errors: Any errors encountered

    Example:
        >>> result = start_discovery_workflow(
        ...     sql="SELECT CASE WHEN acct < 4000 THEN 'Revenue'...",
        ...     target_schema="ANALYTICS"
        ... )
        >>> print(result["execution_id"])
        "exec_abc123"
    """
    from databridge_discovery.workflows.discovery_workflow import (
        DiscoveryWorkflow,
        DiscoveryWorkflowConfig,
    )

    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    # Create workflow config
    config = DiscoveryWorkflowConfig(
        scan_schema=scan_schema,
        extract_logic=extract_logic,
        design_model=design_model,
        generate_dbt=generate_dbt,
        validate_deployment=validate_deployment,
        dry_run=dry_run,
        target_schema=target_schema,
    )

    # Create workflow
    workflow = DiscoveryWorkflow(orchestrator=orchestrator, config=config)

    # Build input data
    input_data = {
        "schema_name": schema_name,
        "target_schema": target_schema,
    }
    if tables:
        input_data["tables"] = tables
    if sql:
        input_data["sql"] = sql

    # Execute workflow
    result = workflow.execute(input_data)

    # Store execution
    _workflow_executions[result.execution_id] = {
        "workflow": workflow,
        "result": result,
        "orchestrator_id": orch_id,
    }

    return {
        "workflow_id": result.workflow_id,
        "execution_id": result.execution_id,
        "orchestrator_id": orch_id,
        "status": result.status.value,
        "phases_completed": result.phases_completed,
        "hierarchy_count": len(result.discovered_hierarchies),
        "model_count": len(result.generated_models),
        "validation_count": len(result.validation_results),
        "error_count": len(result.errors),
        "errors": result.errors[:10] if result.errors else [],
        "duration_seconds": result.duration_seconds,
        "started_at": result.started_at.isoformat() if result.started_at else None,
        "completed_at": result.completed_at.isoformat() if result.completed_at else None,
    }


def get_workflow_status(
    execution_id: str,
) -> dict[str, Any]:
    """
    Get the current status of a workflow execution.

    Returns detailed progress information including completed steps,
    current step, and any errors.

    Args:
        execution_id: Workflow execution ID

    Returns:
        Dictionary containing:
        - execution_id: Execution ID
        - status: Current status
        - current_step: Step being executed
        - steps_completed: Number of completed steps
        - steps_total: Total number of steps
        - progress: Progress percentage (0.0 to 1.0)
        - phase_results: Results from each phase
        - errors: Any errors

    Example:
        >>> status = get_workflow_status("exec_abc123")
        >>> print(status["progress"])
        0.75
    """
    # Check local storage first
    if execution_id in _workflow_executions:
        exec_data = _workflow_executions[execution_id]
        result = exec_data["result"]

        return {
            "execution_id": execution_id,
            "workflow_id": result.workflow_id,
            "status": result.status.value,
            "phases_completed": result.phases_completed,
            "hierarchy_count": len(result.discovered_hierarchies),
            "model_count": len(result.generated_models),
            "error_count": len(result.errors),
            "errors": result.errors[:10] if result.errors else [],
            "duration_seconds": result.duration_seconds,
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
        }

    # Check orchestrators
    for orch_id, orchestrator in _orchestrators.items():
        status = orchestrator.get_workflow_status(execution_id)
        if status:
            return {
                "execution_id": execution_id,
                "orchestrator_id": orch_id,
                **status,
            }

    return {"error": f"Execution not found: {execution_id}"}


def invoke_schema_scanner(
    tables: list[dict[str, Any]],
    capability: str = "scan_schema",
    detect_keys: bool = True,
    sample_profiles: bool = False,
    sample_size: int = 1000,
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Invoke the Schema Scanner agent directly.

    The Schema Scanner extracts technical metadata from database tables
    including columns, types, keys, and data profiles.

    Args:
        tables: List of table metadata dictionaries with columns
        capability: Capability to execute (scan_schema, extract_metadata, detect_keys, sample_profiles)
        detect_keys: Also run key detection
        sample_profiles: Also generate data profiles
        sample_size: Sample size for profiling
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - success: Whether execution succeeded
        - capability: Capability executed
        - tables: Processed table metadata
        - keys_detected: Primary/foreign keys found
        - profiles: Data profiles (if requested)
        - duration_ms: Execution time

    Example:
        >>> result = invoke_schema_scanner(
        ...     tables=[{"name": "orders", "columns": [...]}],
        ...     capability="scan_schema"
        ... )
        >>> print(result["tables"][0]["primary_keys"])
        ["order_id"]
    """
    from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    # Get scanner agent
    scanner = orchestrator.get_agent("scanner")
    if not scanner:
        return {"error": "Schema Scanner agent not registered"}

    # Parse capability
    try:
        cap_enum = AgentCapability(capability)
    except ValueError:
        return {
            "error": f"Invalid capability: {capability}",
            "valid_capabilities": [c.value for c in scanner.get_capabilities()],
        }

    # Create context
    context = TaskContext(
        task_id=f"scanner_{capability}",
        input_data={"tables": tables},
    )

    # Execute
    result = scanner.execute(cap_enum, context)

    response = {
        "success": result.success,
        "capability": capability,
        "agent": "schema_scanner",
        "duration_seconds": result.duration_seconds,
    }

    if result.success:
        response["data"] = result.data
    else:
        response["error"] = result.error

    # Run additional capabilities if requested
    if result.success and detect_keys and capability == "scan_schema":
        key_context = TaskContext(
            task_id="scanner_detect_keys",
            input_data={"tables": result.data.get("tables", tables)},
        )
        key_result = scanner.execute(AgentCapability.DETECT_KEYS, key_context)
        if key_result.success:
            response["keys_detected"] = key_result.data

    if result.success and sample_profiles:
        profile_context = TaskContext(
            task_id="scanner_profile",
            input_data={"tables": result.data.get("tables", tables), "sample_size": sample_size},
        )
        profile_result = scanner.execute(AgentCapability.SAMPLE_PROFILES, profile_context)
        if profile_result.success:
            response["profiles"] = profile_result.data

    return response


def invoke_logic_extractor(
    sql: str,
    capability: str = "parse_sql",
    dialect: str = "snowflake",
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Invoke the Logic Extractor agent directly.

    The Logic Extractor parses SQL and extracts business logic
    including CASE statements, calculations, and aggregations.

    Args:
        sql: SQL statement to analyze
        capability: Capability to execute (parse_sql, extract_case, identify_calcs, detect_aggregations)
        dialect: SQL dialect
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - success: Whether execution succeeded
        - capability: Capability executed
        - parsed_sql: Parsed SQL structure (for parse_sql)
        - case_statements: Extracted CASE statements (for extract_case)
        - calculations: Identified calculations (for identify_calcs)
        - aggregations: Detected aggregations (for detect_aggregations)
        - duration_ms: Execution time

    Example:
        >>> result = invoke_logic_extractor(
        ...     sql="SELECT CASE WHEN type='A' THEN 'Cat A' END",
        ...     capability="extract_case"
        ... )
        >>> print(len(result["case_statements"]))
        1
    """
    from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    # Get extractor agent
    extractor = orchestrator.get_agent("extractor")
    if not extractor:
        return {"error": "Logic Extractor agent not registered"}

    # Parse capability
    try:
        cap_enum = AgentCapability(capability)
    except ValueError:
        return {
            "error": f"Invalid capability: {capability}",
            "valid_capabilities": [c.value for c in extractor.get_capabilities()],
        }

    # Create context
    context = TaskContext(
        task_id=f"extractor_{capability}",
        input_data={"sql": sql, "dialect": dialect},
    )

    # Execute
    result = extractor.execute(cap_enum, context)

    response = {
        "success": result.success,
        "capability": capability,
        "agent": "logic_extractor",
        "duration_seconds": result.duration_seconds,
    }

    if result.success:
        response["data"] = result.data
    else:
        response["error"] = result.error

    return response


def invoke_warehouse_architect(
    tables: list[dict[str, Any]] | None = None,
    relationships: list[dict[str, Any]] | None = None,
    design: dict[str, Any] | None = None,
    capability: str = "design_star_schema",
    target_schema: str = "ANALYTICS",
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Invoke the Warehouse Architect agent directly.

    The Warehouse Architect designs star schemas, generates dimensions
    and facts, and creates dbt models.

    Args:
        tables: Table metadata for star schema design
        relationships: Table relationships
        design: Existing design (for model generation)
        capability: Capability to execute (design_star_schema, generate_dims, generate_facts, dbt_models)
        target_schema: Target schema name
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - success: Whether execution succeeded
        - capability: Capability executed
        - design: Star schema design (for design_star_schema)
        - dimensions: Generated dimensions (for generate_dims)
        - facts: Generated facts (for generate_facts)
        - models: dbt models (for dbt_models)
        - duration_ms: Execution time

    Example:
        >>> result = invoke_warehouse_architect(
        ...     tables=[{"name": "orders", ...}],
        ...     capability="design_star_schema"
        ... )
        >>> print(result["design"]["dimensions"])
        [{"name": "dim_customer", ...}]
    """
    from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    # Get architect agent
    architect = orchestrator.get_agent("architect")
    if not architect:
        return {"error": "Warehouse Architect agent not registered"}

    # Parse capability
    try:
        cap_enum = AgentCapability(capability)
    except ValueError:
        return {
            "error": f"Invalid capability: {capability}",
            "valid_capabilities": [c.value for c in architect.get_capabilities()],
        }

    # Build input data
    input_data = {"target_schema": target_schema}
    if tables:
        input_data["tables"] = tables
    if relationships:
        input_data["relationships"] = relationships
    if design:
        input_data["design"] = design

    # Create context
    context = TaskContext(
        task_id=f"architect_{capability}",
        input_data=input_data,
    )

    # Execute
    result = architect.execute(cap_enum, context)

    response = {
        "success": result.success,
        "capability": capability,
        "agent": "warehouse_architect",
        "duration_seconds": result.duration_seconds,
    }

    if result.success:
        response["data"] = result.data
    else:
        response["error"] = result.error

    return response


def invoke_deploy_validator(
    ddl_statements: list[str] | None = None,
    validations: list[dict[str, Any]] | None = None,
    comparisons: list[dict[str, Any]] | None = None,
    capability: str = "execute_ddl",
    dry_run: bool = True,
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Invoke the Deploy & Validate agent directly.

    The Deploy Validator executes DDL statements, runs dbt,
    and validates data counts and aggregates.

    Args:
        ddl_statements: DDL statements to execute
        validations: Count validations to perform
        comparisons: Aggregate comparisons to perform
        capability: Capability to execute (execute_ddl, run_dbt, validate_counts, compare_aggregates)
        dry_run: Preview mode (validate only, no execution)
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - success: Whether execution succeeded
        - capability: Capability executed
        - executions: DDL execution results (for execute_ddl)
        - validations: Validation results (for validate_counts)
        - comparisons: Comparison results (for compare_aggregates)
        - duration_ms: Execution time

    Example:
        >>> result = invoke_deploy_validator(
        ...     ddl_statements=["CREATE TABLE test (id INT);"],
        ...     capability="execute_ddl",
        ...     dry_run=True
        ... )
        >>> print(result["data"]["success_count"])
        1
    """
    from databridge_discovery.agents.base_agent import AgentCapability, TaskContext

    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    # Get validator agent
    validator = orchestrator.get_agent("validator")
    if not validator:
        return {"error": "Deploy Validator agent not registered"}

    # Parse capability
    try:
        cap_enum = AgentCapability(capability)
    except ValueError:
        return {
            "error": f"Invalid capability: {capability}",
            "valid_capabilities": [c.value for c in validator.get_capabilities()],
        }

    # Build input data
    input_data = {"dry_run": dry_run}
    if ddl_statements:
        input_data["ddl_statements"] = ddl_statements
    if validations:
        input_data["validations"] = validations
    if comparisons:
        input_data["comparisons"] = comparisons

    # Create context
    context = TaskContext(
        task_id=f"validator_{capability}",
        input_data=input_data,
    )

    # Execute
    result = validator.execute(cap_enum, context)

    response = {
        "success": result.success,
        "capability": capability,
        "agent": "deploy_validator",
        "dry_run": dry_run,
        "duration_seconds": result.duration_seconds,
    }

    if result.success:
        response["data"] = result.data
    else:
        response["error"] = result.error

    return response


def pause_workflow(
    execution_id: str,
) -> dict[str, Any]:
    """
    Pause a running workflow execution.

    Pauses the workflow at the current step. Can be resumed later
    using resume_workflow.

    Args:
        execution_id: Workflow execution ID to pause

    Returns:
        Dictionary containing:
        - success: Whether pause succeeded
        - execution_id: Execution ID
        - status: New status (paused)
        - current_step: Step paused at

    Example:
        >>> result = pause_workflow("exec_abc123")
        >>> print(result["status"])
        "paused"
    """
    # Find the orchestrator with this execution
    for orch_id, orchestrator in _orchestrators.items():
        if orchestrator.pause_workflow(execution_id):
            status = orchestrator.get_workflow_status(execution_id)
            return {
                "success": True,
                "execution_id": execution_id,
                "orchestrator_id": orch_id,
                "status": status.get("state", "paused") if status else "paused",
                "current_step": status.get("current_step") if status else None,
            }

    return {
        "success": False,
        "error": f"Execution not found or cannot be paused: {execution_id}",
    }


def resume_workflow(
    execution_id: str,
) -> dict[str, Any]:
    """
    Resume a paused workflow execution.

    Continues execution from where it was paused.

    Args:
        execution_id: Workflow execution ID to resume

    Returns:
        Dictionary containing:
        - success: Whether resume succeeded
        - execution_id: Execution ID
        - status: New status (running)

    Example:
        >>> result = resume_workflow("exec_abc123")
        >>> print(result["status"])
        "running"
    """
    # Find the orchestrator with this execution
    for orch_id, orchestrator in _orchestrators.items():
        if orchestrator.resume_workflow(execution_id):
            status = orchestrator.get_workflow_status(execution_id)
            return {
                "success": True,
                "execution_id": execution_id,
                "orchestrator_id": orch_id,
                "status": status.get("state", "running") if status else "running",
            }

    return {
        "success": False,
        "error": f"Execution not found or cannot be resumed: {execution_id}",
    }


def get_agent_capabilities(
    agent_name: str | None = None,
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Get capabilities for registered agents.

    Lists all capabilities available for each agent type,
    useful for understanding what operations are supported.

    Args:
        agent_name: Specific agent to query (scanner, extractor, architect, validator)
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - agents: List of agents with their capabilities
        - total_capabilities: Total number of capabilities

    Example:
        >>> result = get_agent_capabilities(agent_name="scanner")
        >>> print(result["agents"][0]["capabilities"])
        ["scan_schema", "extract_metadata", "detect_keys", "sample_profiles"]
    """
    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    agents = orchestrator.list_agents()

    if agent_name:
        agents = [a for a in agents if a["name"] == agent_name]
        if not agents:
            return {
                "error": f"Agent not found: {agent_name}",
                "available_agents": [a["name"] for a in orchestrator.list_agents()],
            }

    total_caps = sum(len(a["capabilities"]) for a in agents)

    return {
        "orchestrator_id": orch_id,
        "agents": agents,
        "total_capabilities": total_caps,
    }


def configure_agent(
    agent_name: str,
    config: dict[str, Any],
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Configure an agent's parameters.

    Updates agent configuration like timeouts, batch sizes,
    and behavior settings.

    Args:
        agent_name: Agent to configure (scanner, extractor, architect, validator)
        config: Configuration dictionary with:
            - timeout_seconds: Execution timeout
            - max_retries: Retry count
            - batch_size: Batch processing size
            - Other agent-specific settings
        orchestrator_id: Existing orchestrator to use

    Returns:
        Dictionary containing:
        - success: Whether configuration succeeded
        - agent_name: Agent configured
        - config: Applied configuration

    Example:
        >>> result = configure_agent(
        ...     agent_name="scanner",
        ...     config={"timeout_seconds": 300, "batch_size": 100}
        ... )
        >>> print(result["success"])
        True
    """
    # Get or create orchestrator
    orch_id, orchestrator = _get_or_create_orchestrator(orchestrator_id)

    agent = orchestrator.get_agent(agent_name)
    if not agent:
        return {
            "error": f"Agent not found: {agent_name}",
            "available_agents": [a["name"] for a in orchestrator.list_agents()],
        }

    # Apply configuration
    applied_config = {}
    if "timeout_seconds" in config:
        agent._config.timeout_seconds = config["timeout_seconds"]
        applied_config["timeout_seconds"] = config["timeout_seconds"]
    if "max_retries" in config:
        agent._config.max_retries = config["max_retries"]
        applied_config["max_retries"] = config["max_retries"]

    return {
        "success": True,
        "orchestrator_id": orch_id,
        "agent_name": agent_name,
        "config": applied_config,
    }


def validate_workflow_config(
    scan_schema: bool = True,
    extract_logic: bool = True,
    design_model: bool = True,
    generate_dbt: bool = True,
    validate_deployment: bool = False,
    tables: list[dict[str, Any]] | None = None,
    sql: str | None = None,
) -> dict[str, Any]:
    """
    Validate workflow configuration before execution.

    Checks that all required inputs are present and phases
    are configured correctly.

    Args:
        scan_schema: Include schema scanning phase
        extract_logic: Include logic extraction phase
        design_model: Include star schema design phase
        generate_dbt: Include dbt generation phase
        validate_deployment: Include validation phase
        tables: Table metadata (required for scan_schema)
        sql: SQL query (required for extract_logic)

    Returns:
        Dictionary containing:
        - valid: Whether configuration is valid
        - phases: List of enabled phases
        - errors: Configuration errors
        - warnings: Configuration warnings

    Example:
        >>> result = validate_workflow_config(
        ...     scan_schema=True,
        ...     tables=None  # Missing!
        ... )
        >>> print(result["valid"])
        False
    """
    errors = []
    warnings = []
    phases = []

    # Check phase dependencies
    if scan_schema:
        phases.append("scan")
        if not tables:
            warnings.append("scan_schema enabled but no tables provided")

    if extract_logic:
        phases.append("extract")
        if not sql:
            warnings.append("extract_logic enabled but no SQL provided")

    if design_model:
        phases.append("design")
        if not scan_schema and not tables:
            errors.append("design_model requires scan_schema or tables input")

    if generate_dbt:
        phases.append("generate")
        if not design_model:
            warnings.append("generate_dbt without design_model may produce incomplete results")

    if validate_deployment:
        phases.append("validate")
        if not generate_dbt:
            warnings.append("validate_deployment without generate_dbt has nothing to validate")

    # Check for empty workflow
    if not phases:
        errors.append("No phases enabled - workflow would do nothing")

    # Check input requirements
    if not tables and not sql:
        errors.append("At least one of tables or sql must be provided")

    return {
        "valid": len(errors) == 0,
        "phases": phases,
        "phase_count": len(phases),
        "errors": errors,
        "warnings": warnings,
        "has_tables": tables is not None,
        "has_sql": sql is not None,
    }


def get_workflow_history(
    limit: int = 10,
    orchestrator_id: str | None = None,
) -> dict[str, Any]:
    """
    Get history of workflow executions.

    Returns recent workflow runs with their status and results.

    Args:
        limit: Maximum number of executions to return
        orchestrator_id: Specific orchestrator to query

    Returns:
        Dictionary containing:
        - executions: List of past executions with:
            - execution_id: Execution ID
            - workflow_id: Workflow ID
            - status: Final status
            - started_at: Start time
            - completed_at: End time
            - duration_seconds: Total duration
        - total_count: Total executions found

    Example:
        >>> result = get_workflow_history(limit=5)
        >>> print(len(result["executions"]))
        5
    """
    all_executions = []

    # Collect from specific or all orchestrators
    if orchestrator_id:
        if orchestrator_id in _orchestrators:
            history = _orchestrators[orchestrator_id].get_workflow_history(limit)
            for h in history:
                h["orchestrator_id"] = orchestrator_id
            all_executions.extend(history)
    else:
        for orch_id, orchestrator in _orchestrators.items():
            history = orchestrator.get_workflow_history(limit)
            for h in history:
                h["orchestrator_id"] = orch_id
            all_executions.extend(history)

    # Also include locally tracked executions
    for exec_id, exec_data in _workflow_executions.items():
        result = exec_data["result"]
        all_executions.append({
            "execution_id": exec_id,
            "workflow_id": result.workflow_id,
            "orchestrator_id": exec_data.get("orchestrator_id"),
            "status": result.status.value,
            "phases_completed": result.phases_completed,
            "hierarchy_count": len(result.discovered_hierarchies),
            "model_count": len(result.generated_models),
            "started_at": result.started_at.isoformat() if result.started_at else None,
            "completed_at": result.completed_at.isoformat() if result.completed_at else None,
            "duration_seconds": result.duration_seconds,
        })

    # Sort by start time (most recent first)
    all_executions.sort(
        key=lambda x: x.get("started_at") or "",
        reverse=True,
    )

    return {
        "executions": all_executions[:limit],
        "total_count": len(all_executions),
    }
