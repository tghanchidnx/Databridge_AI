"""
Data Catalog Scanner - Auto-discovery of data assets.

Scans data sources to automatically catalog:
- Databases, schemas, tables, views
- Column metadata and statistics
- PII detection
- Data patterns
"""

import logging
import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

from .types import (
    AssetType,
    ColumnProfile,
    DataAsset,
    DataClassification,
    DataQualityTier,
    QualityMetrics,
    ScanConfig,
    ScanResult,
    Tag,
)
from .catalog_store import CatalogStore

logger = logging.getLogger(__name__)


# PII detection patterns
PII_PATTERNS = {
    "email": (r"email|e_mail|e-mail", DataClassification.PII),
    "phone": (r"phone|mobile|cell|fax|telephone", DataClassification.PII),
    "ssn": (r"ssn|social_security|social_sec", DataClassification.PII),
    "name": (r"first_name|last_name|full_name|customer_name|employee_name", DataClassification.PII),
    "address": (r"address|street|city|zip|postal|state", DataClassification.PII),
    "dob": (r"date_of_birth|dob|birth_date|birthday", DataClassification.PII),
    "credit_card": (r"credit_card|card_number|ccn|pan", DataClassification.PCI),
    "cvv": (r"cvv|cvc|security_code", DataClassification.PCI),
    "password": (r"password|passwd|pwd|secret", DataClassification.RESTRICTED),
    "token": (r"token|api_key|access_key|secret_key", DataClassification.RESTRICTED),
    "salary": (r"salary|wage|compensation|pay_rate", DataClassification.CONFIDENTIAL),
    "medical": (r"diagnosis|medical|health|patient|prescription", DataClassification.PHI),
}

# Data type patterns for smart categorization
DATE_TYPES = {"DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}
NUMERIC_TYPES = {"NUMBER", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE", "REAL", "INTEGER", "INT", "BIGINT", "SMALLINT"}
STRING_TYPES = {"VARCHAR", "TEXT", "STRING", "CHAR", "NVARCHAR", "NCHAR"}


class CatalogScanner:
    """Scans data sources to discover and catalog assets."""

    def __init__(
        self,
        catalog_store: CatalogStore,
        query_func: Optional[Callable[[str, str], List[Dict]]] = None,
    ):
        """
        Initialize the scanner.

        Args:
            catalog_store: The catalog store to populate
            query_func: Function to execute queries (connection_id, sql) -> results
        """
        self.catalog = catalog_store
        self.query_func = query_func
        self._current_scan: Optional[ScanResult] = None

    def scan_connection(self, config: ScanConfig) -> ScanResult:
        """
        Scan a data connection and catalog all discovered assets.

        Args:
            config: Scan configuration

        Returns:
            Scan result with statistics
        """
        if not self.query_func:
            raise ValueError("Query function required for scanning")

        self._current_scan = ScanResult(connection_id=config.connection_id)

        try:
            # Get databases
            databases = self._get_databases(config)
            self._current_scan.databases_scanned = len(databases)

            for db_name in databases:
                # Create database asset
                db_asset = self._create_database_asset(config.connection_id, db_name)

                # Get schemas
                schemas = self._get_schemas(config, db_name)
                self._current_scan.schemas_scanned += len(schemas)

                for schema_name in schemas:
                    # Create schema asset
                    schema_asset = self._create_schema_asset(
                        config.connection_id, db_name, schema_name, db_asset.id
                    )

                    # Get tables
                    tables = self._get_tables(config, db_name, schema_name)
                    self._current_scan.tables_scanned += len(tables)

                    for table_info in tables:
                        # Create table asset
                        table_asset = self._create_table_asset(
                            config, db_name, schema_name, table_info, schema_asset.id
                        )

                        # Get columns if requested
                        if config.include_columns:
                            columns = self._get_columns(config, db_name, schema_name, table_info["name"])
                            self._current_scan.columns_scanned += len(columns)

                            # Profile columns if requested
                            if config.profile_columns:
                                columns = self._profile_columns(
                                    config, db_name, schema_name, table_info["name"], columns
                                )

                            # Detect PII
                            if config.detect_pii:
                                pii_count = self._detect_pii(columns)
                                self._current_scan.pii_columns_detected += pii_count

                            # Update table with columns
                            self.catalog.update_asset(table_asset.id, {
                                "columns": columns,
                            })

            self._current_scan.status = "completed"
            self._current_scan.completed_at = datetime.now()

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            self._current_scan.status = "failed"
            self._current_scan.errors.append(str(e))
            self._current_scan.completed_at = datetime.now()

        return self._current_scan

    def scan_table(
        self,
        connection_id: str,
        database: str,
        schema_name: str,
        table_name: str,
        profile: bool = True,
        detect_pii: bool = True,
    ) -> Optional[DataAsset]:
        """
        Scan a single table and add to catalog.

        Args:
            connection_id: Connection ID
            database: Database name
            schema_name: Schema name
            table_name: Table name
            profile: Whether to collect column statistics
            detect_pii: Whether to detect PII columns

        Returns:
            Created or updated DataAsset
        """
        if not self.query_func:
            raise ValueError("Query function required for scanning")

        config = ScanConfig(
            connection_id=connection_id,
            database=database,
            schema_pattern=schema_name,
            table_pattern=table_name,
            include_columns=True,
            profile_columns=profile,
            detect_pii=detect_pii,
        )

        # Check if table exists
        tables = self._get_tables(config, database, schema_name)
        table_info = next((t for t in tables if t["name"].lower() == table_name.lower()), None)

        if not table_info:
            logger.warning(f"Table not found: {database}.{schema_name}.{table_name}")
            return None

        # Get or create parent assets
        db_asset = self._get_or_create_database(connection_id, database)
        schema_asset = self._get_or_create_schema(connection_id, database, schema_name, db_asset.id)

        # Create table asset
        table_asset = self._create_table_asset(
            config, database, schema_name, table_info, schema_asset.id
        )

        # Get columns
        columns = self._get_columns(config, database, schema_name, table_name)

        if profile:
            columns = self._profile_columns(config, database, schema_name, table_name, columns)

        if detect_pii:
            self._detect_pii(columns)

        # Update with columns
        self.catalog.update_asset(table_asset.id, {"columns": columns})

        return self.catalog.get_asset(table_asset.id)

    def refresh_asset(self, asset_id: str) -> Optional[DataAsset]:
        """
        Refresh metadata for an existing asset.

        Args:
            asset_id: Asset ID to refresh

        Returns:
            Updated asset or None
        """
        asset = self.catalog.get_asset(asset_id)
        if not asset:
            return None

        if asset.asset_type not in (AssetType.TABLE, AssetType.VIEW):
            logger.warning(f"Can only refresh TABLE or VIEW assets, got {asset.asset_type}")
            return asset

        if not asset.source_connection_id or not asset.database or not asset.schema_name:
            logger.warning(f"Asset missing connection info: {asset.name}")
            return asset

        # Re-scan the table
        return self.scan_table(
            connection_id=asset.source_connection_id,
            database=asset.database,
            schema_name=asset.schema_name,
            table_name=asset.name,
            profile=True,
            detect_pii=True,
        )

    # =========================================================================
    # Database Introspection
    # =========================================================================

    def _get_databases(self, config: ScanConfig) -> List[str]:
        """Get list of databases to scan."""
        if config.database:
            return [config.database]

        try:
            sql = "SHOW DATABASES"
            results = self.query_func(config.connection_id, sql)
            return [r.get("name", r.get("DATABASE_NAME", "")) for r in results if r]
        except Exception as e:
            logger.warning(f"Failed to list databases: {e}")
            return []

    def _get_schemas(self, config: ScanConfig, database: str) -> List[str]:
        """Get list of schemas in a database."""
        try:
            sql = f"SHOW SCHEMAS IN DATABASE {database}"
            results = self.query_func(config.connection_id, sql)
            schemas = [r.get("name", r.get("SCHEMA_NAME", "")) for r in results if r]

            # Apply pattern filter
            if config.schema_pattern:
                pattern = config.schema_pattern.replace("%", ".*").replace("_", ".")
                schemas = [s for s in schemas if re.match(pattern, s, re.IGNORECASE)]

            # Exclude system schemas
            system_schemas = {"INFORMATION_SCHEMA", "ACCOUNT_USAGE", "READER_ACCOUNT_USAGE"}
            schemas = [s for s in schemas if s.upper() not in system_schemas]

            return schemas

        except Exception as e:
            logger.warning(f"Failed to list schemas in {database}: {e}")
            return []

    def _get_tables(self, config: ScanConfig, database: str, schema_name: str) -> List[Dict[str, Any]]:
        """Get list of tables and views in a schema."""
        try:
            sql = f"""
            SELECT
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED,
                COMMENT
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}'
            """

            if not config.include_views:
                sql += " AND TABLE_TYPE = 'BASE TABLE'"

            if config.table_pattern:
                pattern = config.table_pattern.replace("*", "%")
                sql += f" AND TABLE_NAME LIKE '{pattern}'"

            sql += " ORDER BY TABLE_NAME"

            results = self.query_func(config.connection_id, sql)

            tables = []
            for r in results:
                tables.append({
                    "name": r.get("TABLE_NAME", r.get("table_name", "")),
                    "type": r.get("TABLE_TYPE", r.get("table_type", "BASE TABLE")),
                    "row_count": r.get("ROW_COUNT", r.get("row_count")),
                    "bytes": r.get("BYTES", r.get("bytes")),
                    "created": r.get("CREATED", r.get("created")),
                    "last_altered": r.get("LAST_ALTERED", r.get("last_altered")),
                    "comment": r.get("COMMENT", r.get("comment")),
                })

            return tables

        except Exception as e:
            logger.warning(f"Failed to list tables in {database}.{schema_name}: {e}")
            return []

    def _get_columns(
        self,
        config: ScanConfig,
        database: str,
        schema_name: str,
        table_name: str,
    ) -> List[ColumnProfile]:
        """Get column metadata for a table."""
        try:
            sql = f"""
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                COLUMN_DEFAULT,
                COMMENT
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}'
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """

            results = self.query_func(config.connection_id, sql)

            columns = []
            for r in results:
                col_name = r.get("COLUMN_NAME", r.get("column_name", ""))
                data_type = r.get("DATA_TYPE", r.get("data_type", ""))
                is_nullable = r.get("IS_NULLABLE", r.get("is_nullable", "YES")) == "YES"
                comment = r.get("COMMENT", r.get("comment"))

                # Detect if likely primary key
                is_pk = col_name.upper() == "ID" or (
                    col_name.upper().endswith("_ID") and col_name.upper().startswith(table_name.upper()[:5])
                )

                # Detect if likely foreign key
                is_fk = col_name.upper().endswith("_ID") and not is_pk

                columns.append(ColumnProfile(
                    column_name=col_name,
                    data_type=data_type,
                    nullable=is_nullable,
                    is_primary_key=is_pk,
                    is_foreign_key=is_fk,
                    description=comment,
                ))

            return columns

        except Exception as e:
            logger.warning(f"Failed to get columns for {database}.{schema_name}.{table_name}: {e}")
            return []

    def _profile_columns(
        self,
        config: ScanConfig,
        database: str,
        schema_name: str,
        table_name: str,
        columns: List[ColumnProfile],
    ) -> List[ColumnProfile]:
        """Profile columns to get statistics."""
        fqn = f"{database}.{schema_name}.{table_name}"

        for col in columns:
            try:
                # Get distinct count and null count
                sql = f"""
                SELECT
                    COUNT(DISTINCT "{col.column_name}") as distinct_count,
                    COUNT(*) - COUNT("{col.column_name}") as null_count,
                    COUNT(*) as total_count
                FROM {fqn}
                """
                result = self.query_func(config.connection_id, sql)
                if result:
                    col.distinct_count = result[0].get("DISTINCT_COUNT", result[0].get("distinct_count"))
                    col.null_count = result[0].get("NULL_COUNT", result[0].get("null_count"))

                # Get min/max for appropriate types
                if col.data_type.upper() in NUMERIC_TYPES | DATE_TYPES:
                    sql = f"""
                    SELECT
                        MIN("{col.column_name}") as min_val,
                        MAX("{col.column_name}") as max_val
                    FROM {fqn}
                    """
                    result = self.query_func(config.connection_id, sql)
                    if result:
                        col.min_value = str(result[0].get("MIN_VAL", result[0].get("min_val", "")))
                        col.max_value = str(result[0].get("MAX_VAL", result[0].get("max_val", "")))

                # Get sample values for string columns
                if col.data_type.upper() in STRING_TYPES and config.sample_size > 0:
                    sql = f"""
                    SELECT DISTINCT "{col.column_name}" as val
                    FROM {fqn}
                    WHERE "{col.column_name}" IS NOT NULL
                    LIMIT 5
                    """
                    result = self.query_func(config.connection_id, sql)
                    if result:
                        col.sample_values = [
                            str(r.get("VAL", r.get("val", "")))
                            for r in result
                        ][:5]

            except Exception as e:
                logger.debug(f"Failed to profile column {col.column_name}: {e}")

        return columns

    def _detect_pii(self, columns: List[ColumnProfile]) -> int:
        """Detect PII in column names and classify them."""
        pii_count = 0

        for col in columns:
            col_name_lower = col.column_name.lower()

            for pii_type, (pattern, classification) in PII_PATTERNS.items():
                if re.search(pattern, col_name_lower):
                    col.classification = classification
                    col.tags.append(Tag(name=pii_type, category="pii"))
                    pii_count += 1
                    break

        return pii_count

    # =========================================================================
    # Asset Creation
    # =========================================================================

    def _create_database_asset(self, connection_id: str, db_name: str) -> DataAsset:
        """Create or update a database asset."""
        existing = self.catalog.get_asset_by_name(db_name, AssetType.DATABASE)

        if existing:
            self.catalog.update_asset(existing.id, {
                "last_scanned_at": datetime.now(),
            })
            return existing

        asset = DataAsset(
            name=db_name,
            asset_type=AssetType.DATABASE,
            database=db_name,
            fully_qualified_name=db_name,
            source_connection_id=connection_id,
            last_scanned_at=datetime.now(),
        )

        self.catalog.create_asset(asset)
        if self._current_scan:
            self._current_scan.assets_created += 1

        return asset

    def _create_schema_asset(
        self,
        connection_id: str,
        db_name: str,
        schema_name: str,
        parent_id: str,
    ) -> DataAsset:
        """Create or update a schema asset."""
        existing = self.catalog.get_asset_by_name(
            schema_name, AssetType.SCHEMA, database=db_name
        )

        if existing:
            self.catalog.update_asset(existing.id, {
                "last_scanned_at": datetime.now(),
            })
            return existing

        asset = DataAsset(
            name=schema_name,
            asset_type=AssetType.SCHEMA,
            database=db_name,
            schema_name=schema_name,
            fully_qualified_name=f"{db_name}.{schema_name}",
            parent_id=parent_id,
            source_connection_id=connection_id,
            last_scanned_at=datetime.now(),
        )

        self.catalog.create_asset(asset)
        if self._current_scan:
            self._current_scan.assets_created += 1

        return asset

    def _create_table_asset(
        self,
        config: ScanConfig,
        db_name: str,
        schema_name: str,
        table_info: Dict[str, Any],
        parent_id: str,
    ) -> DataAsset:
        """Create or update a table/view asset."""
        table_name = table_info["name"]
        table_type = table_info.get("type", "BASE TABLE")
        asset_type = AssetType.VIEW if "VIEW" in table_type.upper() else AssetType.TABLE

        existing = self.catalog.get_asset_by_name(
            table_name, asset_type, database=db_name, schema_name=schema_name
        )

        fqn = f"{db_name}.{schema_name}.{table_name}"

        if existing:
            self.catalog.update_asset(existing.id, {
                "row_count": table_info.get("row_count"),
                "size_bytes": table_info.get("bytes"),
                "description": table_info.get("comment"),
                "last_scanned_at": datetime.now(),
            })
            if self._current_scan:
                self._current_scan.assets_updated += 1
            return existing

        asset = DataAsset(
            name=table_name,
            asset_type=asset_type,
            database=db_name,
            schema_name=schema_name,
            fully_qualified_name=fqn,
            description=table_info.get("comment"),
            row_count=table_info.get("row_count"),
            size_bytes=table_info.get("bytes"),
            parent_id=parent_id,
            source_connection_id=config.connection_id,
            last_scanned_at=datetime.now(),
        )

        self.catalog.create_asset(asset)
        if self._current_scan:
            self._current_scan.assets_created += 1

        return asset

    def _get_or_create_database(self, connection_id: str, db_name: str) -> DataAsset:
        """Get or create a database asset."""
        existing = self.catalog.get_asset_by_name(db_name, AssetType.DATABASE)
        if existing:
            return existing
        return self._create_database_asset(connection_id, db_name)

    def _get_or_create_schema(
        self,
        connection_id: str,
        db_name: str,
        schema_name: str,
        db_id: str,
    ) -> DataAsset:
        """Get or create a schema asset."""
        existing = self.catalog.get_asset_by_name(
            schema_name, AssetType.SCHEMA, database=db_name
        )
        if existing:
            return existing
        return self._create_schema_asset(connection_id, db_name, schema_name, db_id)

    # =========================================================================
    # DataBridge Integration
    # =========================================================================

    def catalog_hierarchy_project(
        self,
        project_id: str,
        hierarchy_service,
    ) -> List[DataAsset]:
        """
        Catalog a DataBridge hierarchy project.

        Args:
            project_id: Hierarchy project ID
            hierarchy_service: HierarchyService instance

        Returns:
            List of created assets
        """
        created_assets = []

        # Get project
        project = hierarchy_service.get_project(project_id)
        if not project:
            raise ValueError(f"Project '{project_id}' not found")

        # Create project asset
        project_asset = DataAsset(
            name=project.get("name", project_id),
            asset_type=AssetType.HIERARCHY_PROJECT,
            description=project.get("description"),
            custom_properties={
                "project_id": project_id,
                "hierarchy_count": 0,
            },
        )
        self.catalog.create_asset(project_asset)
        created_assets.append(project_asset)

        # Get hierarchies
        hierarchies = hierarchy_service.list_hierarchies(project_id)

        for hier in hierarchies:
            hier_asset = DataAsset(
                name=hier.get("name", hier.get("hierarchy_id")),
                asset_type=AssetType.HIERARCHY,
                description=hier.get("description"),
                parent_id=project_asset.id,
                custom_properties={
                    "hierarchy_id": hier.get("hierarchy_id"),
                    "levels": hier.get("levels", {}),
                },
            )
            self.catalog.create_asset(hier_asset)
            created_assets.append(hier_asset)

            # Link to source tables via mappings
            mappings = hierarchy_service.get_source_mappings(
                project_id, hier.get("hierarchy_id")
            )
            for mapping in mappings:
                source_table = mapping.get("source_table")
                if source_table:
                    # Find or create source table asset
                    table_asset = self.catalog.get_asset_by_name(
                        source_table,
                        AssetType.TABLE,
                        database=mapping.get("source_database"),
                        schema_name=mapping.get("source_schema"),
                    )
                    if table_asset:
                        hier_asset.upstream_assets.append(table_asset.id)

            self.catalog.update_asset(hier_asset.id, {
                "upstream_assets": hier_asset.upstream_assets,
            })

        # Update project with hierarchy count
        self.catalog.update_asset(project_asset.id, {
            "custom_properties": {
                **project_asset.custom_properties,
                "hierarchy_count": len(hierarchies),
            },
        })

        return created_assets

    def catalog_semantic_model(
        self,
        model_name: str,
        semantic_model_manager,
    ) -> Optional[DataAsset]:
        """
        Catalog a Cortex Analyst semantic model.

        Args:
            model_name: Semantic model name
            semantic_model_manager: SemanticModelManager instance

        Returns:
            Created asset or None
        """
        model = semantic_model_manager.get_model(model_name)
        if not model:
            return None

        asset = DataAsset(
            name=model.name,
            asset_type=AssetType.SEMANTIC_MODEL,
            description=model.description,
            database=model.database,
            schema_name=model.schema_name,
            custom_properties={
                "table_count": len(model.tables),
                "relationship_count": len(model.relationships),
                "version": model.version,
            },
        )

        # Link to source tables
        for table in model.tables:
            fqn = table.base_table.fully_qualified()
            source_asset = self.catalog.get_asset_by_name(
                table.base_table.table,
                AssetType.TABLE,
                database=table.base_table.database,
                schema_name=table.base_table.schema_name,
            )
            if source_asset:
                asset.upstream_assets.append(source_asset.id)

        self.catalog.create_asset(asset)
        return asset
