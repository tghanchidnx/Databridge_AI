"""
Documentation module for DataBridge Discovery.

This module provides tools for generating:
- Data dictionaries
- Lineage documentation
- Markdown exports
- Diagram generation (Mermaid/D2)
"""

from databridge_discovery.documentation.data_dictionary import (
    DataDictionaryGenerator,
    DataDictionary,
    ColumnDefinition,
    TableDefinition,
    DataType,
    ColumnCategory,
)
from databridge_discovery.documentation.lineage_documenter import (
    LineageDocumenter,
    LineageDiagram,
    LineageNode,
    LineageEdge,
    NodeType,
    EdgeType,
    DiagramFormat,
)
from databridge_discovery.documentation.markdown_exporter import (
    MarkdownExporter,
    ExportConfig,
    ExportResult,
)

__all__ = [
    # Data Dictionary
    "DataDictionaryGenerator",
    "DataDictionary",
    "ColumnDefinition",
    "TableDefinition",
    "DataType",
    "ColumnCategory",
    # Lineage Documenter
    "LineageDocumenter",
    "LineageDiagram",
    "LineageNode",
    "LineageEdge",
    "NodeType",
    "EdgeType",
    "DiagramFormat",
    # Markdown Exporter
    "MarkdownExporter",
    "ExportConfig",
    "ExportResult",
]
