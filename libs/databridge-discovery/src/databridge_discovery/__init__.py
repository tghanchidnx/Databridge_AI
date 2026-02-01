"""
DataBridge Discovery Engine - Automated SQL parsing, CASE extraction, and hierarchy generation.

This library provides tools for:
- Multi-dialect SQL parsing using sqlglot
- CASE WHEN statement extraction with hierarchy detection
- Semantic graph modeling with NetworkX
- Entity type detection (12 standard types)
- Librarian hierarchy project integration
"""

from databridge_discovery.models.parsed_query import (
    ParsedQuery,
    ParsedTable,
    ParsedColumn,
    ParsedJoin,
    QueryMetrics,
)
from databridge_discovery.models.case_statement import (
    CaseStatement,
    CaseCondition,
    CaseWhen,
    ExtractedHierarchy,
    HierarchyLevel,
)
from databridge_discovery.models.session_state import (
    DiscoverySessionState,
    SessionSource,
    ProposedHierarchy,
    DiscoveryEvidence,
)
from databridge_discovery.parser.sql_parser import SQLParser
from databridge_discovery.parser.case_extractor import CaseExtractor
from databridge_discovery.parser.column_resolver import ColumnResolver
from databridge_discovery.session.discovery_session import DiscoverySession
from databridge_discovery.session.result_cache import ResultCache

__version__ = "1.0.0"

__all__ = [
    # Version
    "__version__",
    # Models - Parsed Query
    "ParsedQuery",
    "ParsedTable",
    "ParsedColumn",
    "ParsedJoin",
    "QueryMetrics",
    # Models - CASE Statement
    "CaseStatement",
    "CaseCondition",
    "CaseWhen",
    "ExtractedHierarchy",
    "HierarchyLevel",
    # Models - Session State
    "DiscoverySessionState",
    "SessionSource",
    "ProposedHierarchy",
    "DiscoveryEvidence",
    # Parser
    "SQLParser",
    "CaseExtractor",
    "ColumnResolver",
    # Session
    "DiscoverySession",
    "ResultCache",
]
