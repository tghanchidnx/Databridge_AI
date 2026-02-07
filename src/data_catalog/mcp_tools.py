"""
MCP Tools for Data Catalog.

Provides 15 MCP tools for data catalog management:

Asset Management (5):
- catalog_create_asset: Create a data asset
- catalog_get_asset: Get asset details
- catalog_update_asset: Update asset metadata
- catalog_list_assets: List assets with filters
- catalog_delete_asset: Delete an asset

Discovery & Scanning (3):
- catalog_scan_connection: Scan a data source
- catalog_scan_table: Scan a single table
- catalog_refresh_asset: Refresh asset metadata

Glossary (4):
- catalog_create_term: Create a glossary term
- catalog_get_term: Get term details
- catalog_list_terms: List glossary terms
- catalog_link_term: Link term to asset/column

Search & Discovery (2):
- catalog_search: Search the catalog
- catalog_get_stats: Get catalog statistics

Tags & Classification (1):
- catalog_manage_tags: Add/remove tags from assets
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    AssetType,
    CatalogStats,
    DataAsset,
    DataClassification,
    DataQualityTier,
    GlossaryDomain,
    GlossaryTerm,
    Owner,
    OwnershipRole,
    QualityMetrics,
    ScanConfig,
    SearchQuery,
    Tag,
    TermStatus,
)
from .catalog_store import CatalogStore
from .scanner import CatalogScanner

logger = logging.getLogger(__name__)

# Module-level state
_catalog_store: Optional[CatalogStore] = None
_catalog_scanner: Optional[CatalogScanner] = None


def _get_query_func(settings):
    """Get the query function from connections API."""
    try:
        try:
            from src.connections_api import get_client
        except ImportError:
            from connections_api import get_client

        client = get_client(settings)

        def query_func(connection_id: str, query: str) -> List[Dict]:
            return client.execute_query(connection_id, query)

        return query_func
    except Exception as e:
        logger.warning(f"Failed to get connections API client: {e}")
        return None


def _ensure_catalog(settings) -> CatalogStore:
    """Ensure catalog store is initialized."""
    global _catalog_store
    if _catalog_store is None:
        data_dir = Path(settings.data_dir) / "data_catalog"
        _catalog_store = CatalogStore(data_dir=str(data_dir))
    return _catalog_store


def _ensure_scanner(settings) -> CatalogScanner:
    """Ensure scanner is initialized."""
    global _catalog_scanner
    if _catalog_scanner is None:
        catalog = _ensure_catalog(settings)
        query_func = _get_query_func(settings)
        _catalog_scanner = CatalogScanner(catalog, query_func)
    return _catalog_scanner


def register_data_catalog_tools(mcp, settings):
    """Register all Data Catalog MCP tools."""

    # =========================================================================
    # Asset Management (5)
    # =========================================================================

    @mcp.tool()
    def catalog_create_asset(
        name: str,
        asset_type: str,
        description: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        classification: str = "internal",
        quality_tier: str = "unknown",
        tags: Optional[str] = None,
        owner_name: Optional[str] = None,
        owner_email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new data asset in the catalog.

        Data assets can be databases, schemas, tables, views, hierarchies,
        semantic models, dbt models, or other data objects.

        Args:
            name: Asset name
            asset_type: Type (database, schema, table, view, hierarchy, semantic_model, etc.)
            description: Business description
            database: Database name (for tables/views)
            schema_name: Schema name (for tables/views)
            classification: Data classification (public, internal, confidential, restricted, pii, phi, pci)
            quality_tier: Quality tier (gold, silver, bronze, unknown)
            tags: Comma-separated list of tags
            owner_name: Owner's display name
            owner_email: Owner's email

        Returns:
            Created asset details

        Example:
            catalog_create_asset(
                name="CUSTOMER_DIM",
                asset_type="table",
                database="ANALYTICS",
                schema_name="PUBLIC",
                description="Customer dimension table",
                classification="pii",
                tags="customer,dimension,core"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            # Parse asset type
            try:
                parsed_type = AssetType(asset_type.lower())
            except ValueError:
                return {
                    "error": f"Invalid asset_type: {asset_type}",
                    "valid_types": [t.value for t in AssetType],
                }

            # Create asset
            asset = DataAsset(
                name=name,
                asset_type=parsed_type,
                description=description,
                database=database,
                schema_name=schema_name,
                classification=DataClassification(classification.lower()),
                quality_tier=DataQualityTier(quality_tier.lower()),
            )

            # Set fully qualified name
            if database and schema_name:
                asset.fully_qualified_name = f"{database}.{schema_name}.{name}"
            elif database:
                asset.fully_qualified_name = f"{database}.{name}"

            # Add tags
            if tags:
                for tag_name in tags.split(","):
                    tag_name = tag_name.strip()
                    if tag_name:
                        asset.tags.append(Tag(name=tag_name))

            # Add owner
            if owner_name:
                asset.owners.append(Owner(
                    user_id=owner_email or owner_name.lower().replace(" ", "_"),
                    name=owner_name,
                    email=owner_email,
                    role=OwnershipRole.OWNER,
                ))

            catalog.create_asset(asset)

            return {
                "status": "created",
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.asset_type.value,
                    "fully_qualified_name": asset.fully_qualified_name,
                    "classification": asset.classification.value,
                    "tags": [t.name for t in asset.tags],
                },
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to create asset: {e}")
            return {"error": f"Failed to create asset: {e}"}

    @mcp.tool()
    def catalog_get_asset(
        asset_id: Optional[str] = None,
        name: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get details of a data asset.

        Can look up by ID or by name (with optional database/schema filters).

        Args:
            asset_id: Asset ID (preferred)
            name: Asset name
            database: Filter by database
            schema_name: Filter by schema

        Returns:
            Asset details including columns, tags, owners, quality metrics

        Example:
            catalog_get_asset(name="CUSTOMER_DIM", database="ANALYTICS")
        """
        try:
            catalog = _ensure_catalog(settings)

            if asset_id:
                asset = catalog.get_asset(asset_id)
            elif name:
                asset = catalog.get_asset_by_name(
                    name,
                    database=database,
                    schema_name=schema_name,
                )
            else:
                return {"error": "Either asset_id or name is required"}

            if not asset:
                return {"error": "Asset not found"}

            return {
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "type": asset.asset_type.value,
                    "fully_qualified_name": asset.fully_qualified_name,
                    "description": asset.description,
                    "database": asset.database,
                    "schema": asset.schema_name,
                    "classification": asset.classification.value,
                    "quality_tier": asset.quality_tier.value,
                    "tags": [t.name for t in asset.tags],
                    "owners": [
                        {"name": o.name, "email": o.email, "role": o.role.value}
                        for o in asset.owners
                    ],
                    "columns": [
                        {
                            "name": c.column_name,
                            "type": c.data_type,
                            "nullable": c.nullable,
                            "is_pk": c.is_primary_key,
                            "is_fk": c.is_foreign_key,
                            "description": c.description,
                            "classification": c.classification.value if c.classification else None,
                        }
                        for c in asset.columns
                    ],
                    "row_count": asset.row_count,
                    "size_bytes": asset.size_bytes,
                    "quality_metrics": asset.quality_metrics.model_dump() if asset.quality_metrics else None,
                    "upstream_count": len(asset.upstream_assets),
                    "downstream_count": len(asset.downstream_assets),
                    "created_at": asset.created_at.isoformat() if asset.created_at else None,
                    "last_scanned_at": asset.last_scanned_at.isoformat() if asset.last_scanned_at else None,
                },
            }

        except Exception as e:
            logger.error(f"Failed to get asset: {e}")
            return {"error": f"Failed to get asset: {e}"}

    @mcp.tool()
    def catalog_update_asset(
        asset_id: str,
        description: Optional[str] = None,
        classification: Optional[str] = None,
        quality_tier: Optional[str] = None,
        add_tags: Optional[str] = None,
        remove_tags: Optional[str] = None,
        add_owner: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a data asset's metadata.

        Args:
            asset_id: Asset ID to update
            description: New description
            classification: New classification
            quality_tier: New quality tier
            add_tags: Comma-separated tags to add
            remove_tags: Comma-separated tags to remove
            add_owner: Owner name to add (format: "Name <email>")

        Returns:
            Updated asset summary

        Example:
            catalog_update_asset(
                asset_id="abc-123",
                description="Updated customer dimension",
                classification="confidential",
                add_tags="validated,production"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            updates = {}

            if description is not None:
                updates["description"] = description

            if classification:
                try:
                    updates["classification"] = DataClassification(classification.lower())
                except ValueError:
                    return {"error": f"Invalid classification: {classification}"}

            if quality_tier:
                try:
                    updates["quality_tier"] = DataQualityTier(quality_tier.lower())
                except ValueError:
                    return {"error": f"Invalid quality_tier: {quality_tier}"}

            # Apply basic updates
            if updates:
                catalog.update_asset(asset_id, updates)

            # Add tags
            if add_tags:
                for tag_name in add_tags.split(","):
                    tag_name = tag_name.strip()
                    if tag_name:
                        catalog.add_tag_to_asset(asset_id, tag_name)

            # Remove tags
            if remove_tags:
                for tag_name in remove_tags.split(","):
                    tag_name = tag_name.strip()
                    if tag_name:
                        catalog.remove_tag_from_asset(asset_id, tag_name)

            # Add owner
            if add_owner:
                # Parse "Name <email>" format
                import re
                match = re.match(r"(.+?)\s*<(.+?)>", add_owner)
                if match:
                    name, email = match.groups()
                else:
                    name = add_owner
                    email = None

                asset = catalog.get_asset(asset_id)
                if asset:
                    asset.add_owner(Owner(
                        user_id=email or name.lower().replace(" ", "_"),
                        name=name.strip(),
                        email=email,
                        role=OwnershipRole.OWNER,
                    ))
                    catalog.update_asset(asset_id, {"owners": asset.owners})

            asset = catalog.get_asset(asset_id)
            if not asset:
                return {"error": "Asset not found"}

            return {
                "status": "updated",
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "classification": asset.classification.value,
                    "quality_tier": asset.quality_tier.value,
                    "tags": [t.name for t in asset.tags],
                    "owners": [o.name for o in asset.owners],
                },
            }

        except Exception as e:
            logger.error(f"Failed to update asset: {e}")
            return {"error": f"Failed to update asset: {e}"}

    @mcp.tool()
    def catalog_list_assets(
        asset_type: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        tags: Optional[str] = None,
        classification: Optional[str] = None,
        quality_tier: Optional[str] = None,
        owner_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List data assets with optional filters.

        Args:
            asset_type: Filter by type (table, view, hierarchy, etc.)
            database: Filter by database
            schema_name: Filter by schema
            tags: Comma-separated required tags
            classification: Filter by classification
            quality_tier: Filter by quality tier
            owner_id: Filter by owner user_id
            limit: Maximum results (default 50)
            offset: Pagination offset

        Returns:
            List of assets matching filters

        Example:
            catalog_list_assets(
                asset_type="table",
                database="ANALYTICS",
                tags="customer,validated"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            # Parse filters
            parsed_type = AssetType(asset_type.lower()) if asset_type else None
            parsed_tags = [t.strip() for t in tags.split(",")] if tags else None
            parsed_classification = DataClassification(classification.lower()) if classification else None
            parsed_tier = DataQualityTier(quality_tier.lower()) if quality_tier else None

            assets = catalog.list_assets(
                asset_type=parsed_type,
                database=database,
                schema_name=schema_name,
                tags=parsed_tags,
                classification=parsed_classification,
                quality_tier=parsed_tier,
                owner_id=owner_id,
                limit=limit,
                offset=offset,
            )

            return {
                "count": len(assets),
                "offset": offset,
                "limit": limit,
                "assets": [
                    {
                        "id": a.id,
                        "name": a.name,
                        "type": a.asset_type.value,
                        "fully_qualified_name": a.fully_qualified_name,
                        "classification": a.classification.value,
                        "quality_tier": a.quality_tier.value,
                        "tags": [t.name for t in a.tags],
                        "row_count": a.row_count,
                    }
                    for a in assets
                ],
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to list assets: {e}")
            return {"error": f"Failed to list assets: {e}"}

    @mcp.tool()
    def catalog_delete_asset(asset_id: str) -> Dict[str, Any]:
        """
        Delete a data asset from the catalog.

        Args:
            asset_id: Asset ID to delete

        Returns:
            Deletion status

        Example:
            catalog_delete_asset(asset_id="abc-123")
        """
        try:
            catalog = _ensure_catalog(settings)

            asset = catalog.get_asset(asset_id)
            if not asset:
                return {"error": "Asset not found"}

            name = asset.name
            if catalog.delete_asset(asset_id):
                return {
                    "status": "deleted",
                    "asset_name": name,
                }
            else:
                return {"error": "Failed to delete asset"}

        except Exception as e:
            logger.error(f"Failed to delete asset: {e}")
            return {"error": f"Failed to delete asset: {e}"}

    # =========================================================================
    # Discovery & Scanning (3)
    # =========================================================================

    @mcp.tool()
    def catalog_scan_connection(
        connection_id: str,
        database: Optional[str] = None,
        schema_pattern: Optional[str] = None,
        table_pattern: Optional[str] = None,
        include_views: bool = True,
        profile_columns: bool = False,
        detect_pii: bool = True,
    ) -> Dict[str, Any]:
        """
        Scan a data connection and catalog discovered assets.

        Automatically discovers databases, schemas, tables, and columns.
        Optionally profiles data and detects PII columns.

        Args:
            connection_id: Snowflake connection ID
            database: Specific database to scan (default: all)
            schema_pattern: Schema name pattern (e.g., "PROD_%")
            table_pattern: Table name pattern (e.g., "DIM_%")
            include_views: Include views in scan
            profile_columns: Collect column statistics (slower)
            detect_pii: Detect PII columns by name patterns

        Returns:
            Scan results with statistics

        Example:
            catalog_scan_connection(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_pattern="PUBLIC",
                detect_pii=True
            )
        """
        try:
            scanner = _ensure_scanner(settings)

            config = ScanConfig(
                connection_id=connection_id,
                database=database,
                schema_pattern=schema_pattern,
                table_pattern=table_pattern,
                include_views=include_views,
                include_columns=True,
                profile_columns=profile_columns,
                detect_pii=detect_pii,
            )

            result = scanner.scan_connection(config)

            return {
                "scan_id": result.scan_id,
                "status": result.status,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "statistics": {
                    "databases_scanned": result.databases_scanned,
                    "schemas_scanned": result.schemas_scanned,
                    "tables_scanned": result.tables_scanned,
                    "columns_scanned": result.columns_scanned,
                    "assets_created": result.assets_created,
                    "assets_updated": result.assets_updated,
                    "pii_columns_detected": result.pii_columns_detected,
                },
                "errors": result.errors,
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Scan failed: {e}")
            return {"error": f"Scan failed: {e}"}

    @mcp.tool()
    def catalog_scan_table(
        connection_id: str,
        database: str,
        schema_name: str,
        table_name: str,
        profile: bool = True,
        detect_pii: bool = True,
    ) -> Dict[str, Any]:
        """
        Scan a single table and add to catalog.

        Args:
            connection_id: Snowflake connection ID
            database: Database name
            schema_name: Schema name
            table_name: Table name
            profile: Collect column statistics
            detect_pii: Detect PII columns

        Returns:
            Created/updated asset details

        Example:
            catalog_scan_table(
                connection_id="snowflake-prod",
                database="ANALYTICS",
                schema_name="PUBLIC",
                table_name="CUSTOMER_DIM"
            )
        """
        try:
            scanner = _ensure_scanner(settings)

            asset = scanner.scan_table(
                connection_id=connection_id,
                database=database,
                schema_name=schema_name,
                table_name=table_name,
                profile=profile,
                detect_pii=detect_pii,
            )

            if not asset:
                return {"error": "Table not found or scan failed"}

            return {
                "status": "scanned",
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "fully_qualified_name": asset.fully_qualified_name,
                    "column_count": len(asset.columns),
                    "row_count": asset.row_count,
                    "pii_columns": [
                        c.column_name for c in asset.columns
                        if c.classification in (DataClassification.PII, DataClassification.PHI, DataClassification.PCI)
                    ],
                },
            }

        except Exception as e:
            logger.error(f"Table scan failed: {e}")
            return {"error": f"Table scan failed: {e}"}

    @mcp.tool()
    def catalog_refresh_asset(asset_id: str) -> Dict[str, Any]:
        """
        Refresh metadata for an existing cataloged asset.

        Re-scans the source to update column info, row counts, etc.

        Args:
            asset_id: Asset ID to refresh

        Returns:
            Updated asset details

        Example:
            catalog_refresh_asset(asset_id="abc-123")
        """
        try:
            scanner = _ensure_scanner(settings)

            asset = scanner.refresh_asset(asset_id)
            if not asset:
                return {"error": "Asset not found or refresh failed"}

            return {
                "status": "refreshed",
                "asset": {
                    "id": asset.id,
                    "name": asset.name,
                    "column_count": len(asset.columns),
                    "row_count": asset.row_count,
                    "last_scanned_at": asset.last_scanned_at.isoformat() if asset.last_scanned_at else None,
                },
            }

        except Exception as e:
            logger.error(f"Refresh failed: {e}")
            return {"error": f"Refresh failed: {e}"}

    # =========================================================================
    # Glossary (4)
    # =========================================================================

    @mcp.tool()
    def catalog_create_term(
        name: str,
        definition: str,
        domain: Optional[str] = None,
        category: Optional[str] = None,
        synonyms: Optional[str] = None,
        examples: Optional[str] = None,
        owner_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a business glossary term.

        Glossary terms define business concepts and can be linked to
        data assets and columns.

        Args:
            name: Term name
            definition: Business definition
            domain: Business domain (e.g., "Finance", "Sales")
            category: Term category
            synonyms: Comma-separated synonyms
            examples: Comma-separated examples
            owner_name: Term owner name

        Returns:
            Created term details

        Example:
            catalog_create_term(
                name="Revenue",
                definition="Total income from sales of goods and services",
                domain="Finance",
                synonyms="Sales,Income",
                examples="Product Revenue,Service Revenue"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            term = GlossaryTerm(
                name=name,
                definition=definition,
                domain=domain,
                category=category,
                status=TermStatus.DRAFT,
            )

            if synonyms:
                term.synonyms = [s.strip() for s in synonyms.split(",")]

            if examples:
                term.examples = [e.strip() for e in examples.split(",")]

            if owner_name:
                term.owner = Owner(
                    user_id=owner_name.lower().replace(" ", "_"),
                    name=owner_name,
                    role=OwnershipRole.OWNER,
                )

            catalog.create_term(term)

            return {
                "status": "created",
                "term": {
                    "id": term.id,
                    "name": term.name,
                    "domain": term.domain,
                    "status": term.status.value,
                },
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to create term: {e}")
            return {"error": f"Failed to create term: {e}"}

    @mcp.tool()
    def catalog_get_term(
        term_id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get glossary term details.

        Args:
            term_id: Term ID (preferred)
            name: Term name

        Returns:
            Term details including linked assets

        Example:
            catalog_get_term(name="Revenue")
        """
        try:
            catalog = _ensure_catalog(settings)

            if term_id:
                term = catalog.get_term(term_id)
            elif name:
                term = catalog.get_term_by_name(name)
            else:
                return {"error": "Either term_id or name is required"}

            if not term:
                return {"error": "Term not found"}

            return {
                "term": {
                    "id": term.id,
                    "name": term.name,
                    "definition": term.definition,
                    "domain": term.domain,
                    "category": term.category,
                    "status": term.status.value,
                    "synonyms": term.synonyms,
                    "examples": term.examples,
                    "linked_assets": term.linked_asset_ids,
                    "linked_columns": term.linked_column_refs,
                    "owner": term.owner.name if term.owner else None,
                    "created_at": term.created_at.isoformat() if term.created_at else None,
                },
            }

        except Exception as e:
            logger.error(f"Failed to get term: {e}")
            return {"error": f"Failed to get term: {e}"}

    @mcp.tool()
    def catalog_list_terms(
        domain: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        List glossary terms with filters.

        Args:
            domain: Filter by domain
            status: Filter by status (draft, pending_review, approved, deprecated)
            limit: Maximum results
            offset: Pagination offset

        Returns:
            List of glossary terms

        Example:
            catalog_list_terms(domain="Finance", status="approved")
        """
        try:
            catalog = _ensure_catalog(settings)

            parsed_status = TermStatus(status.lower()) if status else None

            terms = catalog.list_terms(
                domain=domain,
                status=parsed_status,
                limit=limit,
                offset=offset,
            )

            return {
                "count": len(terms),
                "offset": offset,
                "limit": limit,
                "terms": [
                    {
                        "id": t.id,
                        "name": t.name,
                        "domain": t.domain,
                        "status": t.status.value,
                        "linked_assets": len(t.linked_asset_ids),
                    }
                    for t in terms
                ],
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Failed to list terms: {e}")
            return {"error": f"Failed to list terms: {e}"}

    @mcp.tool()
    def catalog_link_term(
        term_id: str,
        asset_id: Optional[str] = None,
        column_ref: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Link a glossary term to an asset or column.

        Args:
            term_id: Glossary term ID
            asset_id: Asset ID to link
            column_ref: Column reference (database.schema.table.column)

        Returns:
            Link status

        Example:
            catalog_link_term(
                term_id="term-123",
                column_ref="ANALYTICS.PUBLIC.SALES.REVENUE"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            if not asset_id and not column_ref:
                return {"error": "Either asset_id or column_ref is required"}

            if asset_id:
                if catalog.link_term_to_asset(term_id, asset_id):
                    return {"status": "linked", "term_id": term_id, "asset_id": asset_id}
                else:
                    return {"error": "Failed to link term to asset"}

            if column_ref:
                if catalog.link_term_to_column(term_id, column_ref):
                    return {"status": "linked", "term_id": term_id, "column_ref": column_ref}
                else:
                    return {"error": "Failed to link term to column"}

        except Exception as e:
            logger.error(f"Failed to link term: {e}")
            return {"error": f"Failed to link term: {e}"}

    # =========================================================================
    # Search & Discovery (2)
    # =========================================================================

    @mcp.tool()
    def catalog_search(
        query: str,
        asset_types: Optional[str] = None,
        tags: Optional[str] = None,
        databases: Optional[str] = None,
        classification: Optional[str] = None,
        include_glossary: bool = True,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Search the data catalog.

        Searches asset names, descriptions, column names, and glossary terms.

        Args:
            query: Search text
            asset_types: Comma-separated asset types to include
            tags: Comma-separated required tags
            databases: Comma-separated database filter
            classification: Filter by classification
            include_glossary: Include glossary terms in results
            limit: Maximum results

        Returns:
            Search results ranked by relevance

        Example:
            catalog_search(
                query="customer revenue",
                asset_types="table,view",
                databases="ANALYTICS"
            )
        """
        try:
            catalog = _ensure_catalog(settings)

            # Parse filters
            parsed_types = None
            if asset_types:
                parsed_types = [AssetType(t.strip().lower()) for t in asset_types.split(",")]

            parsed_tags = [t.strip() for t in tags.split(",")] if tags else None
            parsed_dbs = [d.strip() for d in databases.split(",")] if databases else None
            parsed_class = [DataClassification(classification.lower())] if classification else None

            search_query = SearchQuery(
                query=query,
                asset_types=parsed_types,
                tags=parsed_tags,
                databases=parsed_dbs,
                classifications=parsed_class,
                include_glossary=include_glossary,
                limit=limit,
            )

            results = catalog.search(search_query)

            return {
                "query": results.query,
                "total_count": results.total_count,
                "took_ms": results.took_ms,
                "results": [
                    {
                        "id": r.asset_id,
                        "type": r.asset_type.value,
                        "name": r.name,
                        "fully_qualified_name": r.fully_qualified_name,
                        "description": r.description,
                        "score": round(r.match_score, 3),
                        "highlights": r.match_highlights,
                        "tags": r.tags,
                    }
                    for r in results.results
                ],
            }

        except ValueError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {"error": f"Search failed: {e}"}

    @mcp.tool()
    def catalog_get_stats() -> Dict[str, Any]:
        """
        Get data catalog statistics.

        Returns summary of assets, glossary terms, tags, and data quality.

        Returns:
            Catalog statistics

        Example:
            catalog_get_stats()
        """
        try:
            catalog = _ensure_catalog(settings)
            stats = catalog.get_stats()

            return {
                "assets": {
                    "total": stats.total_assets,
                    "by_type": stats.assets_by_type,
                    "by_classification": stats.assets_by_classification,
                    "by_quality_tier": stats.assets_by_quality_tier,
                    "without_owners": stats.assets_without_owners,
                    "without_descriptions": stats.assets_without_descriptions,
                },
                "glossary": {
                    "total_terms": stats.total_glossary_terms,
                    "by_status": stats.terms_by_status,
                    "by_domain": stats.terms_by_domain,
                },
                "tags": {
                    "total": stats.total_tags,
                    "most_used": stats.most_used_tags[:5],
                },
                "owners": {
                    "total": stats.total_owners,
                },
                "catalog_updated_at": stats.catalog_updated_at.isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {"error": f"Failed to get stats: {e}"}

    # =========================================================================
    # Tags & Classification (1)
    # =========================================================================

    @mcp.tool()
    def catalog_manage_tags(
        action: str,
        asset_id: Optional[str] = None,
        tag_name: Optional[str] = None,
        tag_category: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Manage tags in the catalog.

        Actions:
        - "list": List all tags
        - "add": Add a tag to an asset
        - "remove": Remove a tag from an asset
        - "create": Create a new tag definition

        Args:
            action: Action to perform (list, add, remove, create)
            asset_id: Asset ID (for add/remove)
            tag_name: Tag name
            tag_category: Tag category (for create)

        Returns:
            Action result

        Example:
            catalog_manage_tags(action="add", asset_id="abc-123", tag_name="validated")
        """
        try:
            catalog = _ensure_catalog(settings)

            if action == "list":
                tags = catalog.list_tags(category=tag_category)
                return {
                    "count": len(tags),
                    "tags": [
                        {"name": t.name, "category": t.category, "color": t.color}
                        for t in tags
                    ],
                }

            elif action == "create":
                if not tag_name:
                    return {"error": "tag_name is required"}

                tag = Tag(name=tag_name, category=tag_category)
                catalog.create_tag(tag)
                return {"status": "created", "tag": tag_name}

            elif action == "add":
                if not asset_id or not tag_name:
                    return {"error": "asset_id and tag_name are required"}

                if catalog.add_tag_to_asset(asset_id, tag_name):
                    return {"status": "added", "asset_id": asset_id, "tag": tag_name}
                else:
                    return {"error": "Failed to add tag"}

            elif action == "remove":
                if not asset_id or not tag_name:
                    return {"error": "asset_id and tag_name are required"}

                if catalog.remove_tag_from_asset(asset_id, tag_name):
                    return {"status": "removed", "asset_id": asset_id, "tag": tag_name}
                else:
                    return {"error": "Failed to remove tag"}

            else:
                return {
                    "error": f"Unknown action: {action}",
                    "valid_actions": ["list", "create", "add", "remove"],
                }

        except Exception as e:
            logger.error(f"Tag operation failed: {e}")
            return {"error": f"Tag operation failed: {e}"}

    logger.info("Registered 15 Data Catalog MCP tools")
    return {
        "tools_registered": 15,
        "categories": {
            "asset_management": [
                "catalog_create_asset",
                "catalog_get_asset",
                "catalog_update_asset",
                "catalog_list_assets",
                "catalog_delete_asset",
            ],
            "discovery_scanning": [
                "catalog_scan_connection",
                "catalog_scan_table",
                "catalog_refresh_asset",
            ],
            "glossary": [
                "catalog_create_term",
                "catalog_get_term",
                "catalog_list_terms",
                "catalog_link_term",
            ],
            "search_discovery": [
                "catalog_search",
                "catalog_get_stats",
            ],
            "tags_classification": [
                "catalog_manage_tags",
            ],
        },
    }
