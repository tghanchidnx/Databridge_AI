"""
MCP (Model Context Protocol) tools for the DataBridge Discovery Engine.

Phase 1 - SQL Parser & Session Foundation (6 tools)
Phase 2 - Semantic Graph & Embeddings (8 tools)
Phase 3 - Hierarchy Extraction (10 tools)

Total: 24 MCP tools
"""

from databridge_discovery.mcp.tools import (
    # Registration
    register_discovery_tools,
    # Phase 1 Tools
    parse_sql,
    extract_case_statements,
    analyze_sql_complexity,
    start_discovery_session,
    get_discovery_session,
    export_discovery_evidence,
    add_sql_to_session,
    approve_hierarchy,
    reject_hierarchy,
    export_librarian_csv,
    # Phase 2 Tools
    build_semantic_graph,
    add_graph_relationship,
    find_join_paths,
    analyze_graph_centrality,
    embed_schema_element,
    search_similar_schemas,
    consolidate_entities,
    export_semantic_model,
    get_graph_summary,
    list_graphs,
    delete_graph,
    # Phase 3 Tools
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
    list_hierarchies,
    get_hierarchy_details,
    delete_hierarchy,
)

__all__ = [
    # Registration
    "register_discovery_tools",
    # Phase 1 Tools
    "parse_sql",
    "extract_case_statements",
    "analyze_sql_complexity",
    "start_discovery_session",
    "get_discovery_session",
    "export_discovery_evidence",
    "add_sql_to_session",
    "approve_hierarchy",
    "reject_hierarchy",
    "export_librarian_csv",
    # Phase 2 Tools
    "build_semantic_graph",
    "add_graph_relationship",
    "find_join_paths",
    "analyze_graph_centrality",
    "embed_schema_element",
    "search_similar_schemas",
    "consolidate_entities",
    "export_semantic_model",
    "get_graph_summary",
    "list_graphs",
    "delete_graph",
    # Phase 3 Tools
    "extract_hierarchy_from_sql",
    "analyze_csv_for_hierarchy",
    "detect_entity_types",
    "infer_hierarchy_levels",
    "generate_sort_orders",
    "merge_with_librarian_hierarchy",
    "export_discovery_as_csv",
    "validate_hierarchy_structure",
    "suggest_parent_child_relationships",
    "compare_hierarchies",
    "list_hierarchies",
    "get_hierarchy_details",
    "delete_hierarchy",
]
