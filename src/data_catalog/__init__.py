"""
Data Catalog - Centralized metadata registry for DataBridge.

This module provides:
1. Data Asset Registry - Catalog tables, views, hierarchies, semantic models
2. Business Glossary - Define and manage business terms
3. Data Discovery - Scan data sources to auto-catalog assets
4. Search & Discovery - Find assets by name, description, tags
5. Data Ownership - Track owners, stewards, and access policies
6. Data Classification - Classify data by sensitivity (PII, PHI, PCI)
7. Quality Metrics - Track data quality scores

Phase 29 of DataBridge AI.

MCP Tools (15):
- Asset Management: catalog_create_asset, catalog_get_asset, catalog_update_asset,
                   catalog_list_assets, catalog_delete_asset
- Discovery: catalog_scan_connection, catalog_scan_table, catalog_refresh_asset
- Glossary: catalog_create_term, catalog_get_term, catalog_list_terms, catalog_link_term
- Search: catalog_search, catalog_get_stats
- Tags: catalog_manage_tags
"""

from .types import (
    # Enums
    AssetType,
    DataClassification,
    DataQualityTier,
    OwnershipRole,
    TermStatus,
    # Core models
    Tag,
    Owner,
    QualityMetrics,
    ColumnProfile,
    DataAsset,
    # Glossary
    GlossaryTerm,
    GlossaryDomain,
    # Search
    SearchQuery,
    SearchResult,
    SearchResults,
    # Scanning
    ScanConfig,
    ScanResult,
    # Stats
    CatalogStats,
)

from .catalog_store import CatalogStore
from .scanner import CatalogScanner
from .mcp_tools import register_data_catalog_tools

__all__ = [
    # Enums
    "AssetType",
    "DataClassification",
    "DataQualityTier",
    "OwnershipRole",
    "TermStatus",
    # Core models
    "Tag",
    "Owner",
    "QualityMetrics",
    "ColumnProfile",
    "DataAsset",
    # Glossary
    "GlossaryTerm",
    "GlossaryDomain",
    # Search
    "SearchQuery",
    "SearchResult",
    "SearchResults",
    # Scanning
    "ScanConfig",
    "ScanResult",
    # Stats
    "CatalogStats",
    # Classes
    "CatalogStore",
    "CatalogScanner",
    # Registration
    "register_data_catalog_tools",
]
