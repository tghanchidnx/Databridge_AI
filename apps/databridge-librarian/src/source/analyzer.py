"""
Source analyzer for inferring canonical data models.

Uses database adapters to introspect schemas and infer
entity types, relationships, and column roles.
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    CanonicalModel,
    SourceTable,
    SourceColumn,
    SourceEntity,
    SourceRelationship,
    EntityType,
    RelationshipType,
    ColumnRole,
    SCDType,
)


class SourceAnalyzer:
    """
    Analyzes source databases to infer canonical data models.

    Uses heuristics based on naming patterns, data types, and
    structural relationships to classify entities and columns.

    Example:
        ```python
        from src.connections.adapters import SnowflakeAdapter

        adapter = SnowflakeAdapter(...)
        analyzer = SourceAnalyzer(adapter)

        model = analyzer.analyze_schema(
            database="ANALYTICS",
            schema="PUBLIC",
            name="Sales Analysis"
        )

        print(f"Found {len(model.entities)} entities")
        print(f"Found {len(model.relationships)} relationships")
        ```
    """

    # Entity type patterns (column/table name patterns)
    ENTITY_PATTERNS = {
        EntityType.CUSTOMER: [
            r"customer", r"cust_", r"_cust$", r"client", r"buyer"
        ],
        EntityType.VENDOR: [
            r"vendor", r"vend_", r"_vend$", r"supplier", r"supp_"
        ],
        EntityType.EMPLOYEE: [
            r"employee", r"emp_", r"_emp$", r"staff", r"worker"
        ],
        EntityType.PRODUCT: [
            r"product", r"prod_", r"_prod$", r"item", r"sku"
        ],
        EntityType.INVENTORY: [
            r"inventory", r"inv_", r"stock"
        ],
        EntityType.ASSET: [
            r"asset", r"equipment", r"equip_"
        ],
        EntityType.DEPARTMENT: [
            r"department", r"dept_", r"_dept$", r"division"
        ],
        EntityType.COST_CENTER: [
            r"cost_?center", r"cc_", r"_cc$", r"profit_?center"
        ],
        EntityType.LOCATION: [
            r"location", r"loc_", r"_loc$", r"site", r"facility", r"warehouse"
        ],
        EntityType.COMPANY: [
            r"company", r"corp", r"entity", r"legal_entity", r"org"
        ],
        EntityType.CHART_OF_ACCOUNTS: [
            r"chart.*account", r"coa", r"gl_?account", r"account_?code"
        ],
        EntityType.ACCOUNT: [
            r"account", r"acct_", r"_acct$", r"ledger"
        ],
        EntityType.DATE: [
            r"^date$", r"dim_?date", r"calendar", r"period"
        ],
        EntityType.TRANSACTION: [
            r"transaction", r"trans_", r"_trans$", r"journal", r"entry"
        ],
    }

    # Column role patterns
    ROLE_PATTERNS = {
        ColumnRole.KEY: [
            r"_id$", r"^id$", r"_key$", r"_code$", r"_num$", r"_no$"
        ],
        ColumnRole.DATE: [
            r"_date$", r"_dt$", r"_time$", r"_ts$", r"created", r"modified",
            r"effective", r"expir"
        ],
        ColumnRole.MEASURE: [
            r"amount", r"amt$", r"quantity", r"qty$", r"price", r"cost",
            r"revenue", r"sales", r"total", r"sum", r"count", r"balance"
        ],
        ColumnRole.FLAG: [
            r"_flag$", r"_flg$", r"is_", r"has_", r"_ind$", r"_indicator$",
            r"active", r"enabled", r"deleted"
        ],
        ColumnRole.ATTRIBUTE: [
            r"_name$", r"_desc", r"_title", r"_text", r"_note", r"comment",
            r"address", r"phone", r"email"
        ],
    }

    # SCD Type 2 indicator columns
    SCD2_INDICATORS = [
        "effective_date", "effective_from", "start_date", "valid_from",
        "expiration_date", "effective_to", "end_date", "valid_to",
        "is_current", "current_flag", "version", "version_number"
    ]

    def __init__(self, adapter=None):
        """
        Initialize the analyzer.

        Args:
            adapter: Database adapter implementing AbstractDatabaseAdapter.
                    If None, must be set before calling analyze methods.
        """
        self.adapter = adapter

    def analyze_schema(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        name: str = "Analyzed Model",
        description: str = "",
        include_views: bool = True,
        sample_size: int = 100,
    ) -> CanonicalModel:
        """
        Analyze a database schema and create a canonical model.

        Args:
            database: Database to analyze.
            schema: Schema to analyze.
            name: Name for the resulting model.
            description: Description for the model.
            include_views: Include views in analysis.
            sample_size: Number of sample values to retrieve per column.

        Returns:
            CanonicalModel with inferred structure.
        """
        if not self.adapter:
            raise ValueError("No database adapter configured")

        model = CanonicalModel(
            name=name,
            description=description,
            status="draft",
        )

        # Get tables from the schema
        tables_info = self.adapter.list_tables(database=database, schema=schema)

        for table_info in tables_info:
            # Skip views if not included
            if not include_views and table_info.table_type == "VIEW":
                continue

            # Analyze the table
            source_table = self._analyze_table(
                table_info.name,
                table_info.database,
                table_info.schema,
                table_info.table_type,
                table_info.row_count,
                sample_size,
            )
            model.tables.append(source_table)

        # Infer entities from tables
        model.entities = self._infer_entities(model.tables)

        # Infer relationships
        model.relationships = self._infer_relationships(model.tables)

        model.analyzed_at = datetime.now(timezone.utc)
        return model

    def _analyze_table(
        self,
        table_name: str,
        database: str,
        schema: str,
        table_type: str,
        row_count: Optional[int],
        sample_size: int,
    ) -> SourceTable:
        """Analyze a single table."""
        table = SourceTable(
            name=table_name,
            database=database,
            schema=schema,
            table_type=table_type,
            row_count=row_count,
        )

        # Get columns
        columns_info = self.adapter.list_columns(table_name, database, schema)

        for col_info in columns_info:
            source_col = SourceColumn(
                name=col_info.name,
                data_type=col_info.data_type,
                source_table=table_name,
                source_schema=schema,
                source_database=database,
                nullable=col_info.nullable,
                is_primary_key=col_info.is_primary_key,
            )

            # Infer column role
            source_col.role, source_col.confidence = self._infer_column_role(
                col_info.name, col_info.data_type
            )

            # Infer entity type from column name
            entity_type, entity_confidence = self._infer_entity_type(col_info.name)
            if entity_type:
                source_col.entity_type = entity_type
                source_col.confidence = max(source_col.confidence, entity_confidence)

            # Check for foreign key patterns
            fk_ref = self._detect_foreign_key(col_info.name, table_name)
            if fk_ref:
                source_col.is_foreign_key = True
                source_col.foreign_key_reference = fk_ref

            table.columns.append(source_col)

        # Infer table entity type
        table.entity_type, table.confidence = self._infer_table_entity_type(table)

        # Check for SCD Type 2
        table.scd_type = self._detect_scd_type(table)

        return table

    def _infer_column_role(
        self, column_name: str, data_type: str
    ) -> Tuple[ColumnRole, float]:
        """Infer the role of a column."""
        name_lower = column_name.lower()
        type_lower = data_type.lower()

        # Check patterns
        for role, patterns in self.ROLE_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name_lower, re.IGNORECASE):
                    return role, 0.8

        # Check data type
        if any(t in type_lower for t in ["int", "number", "decimal", "float", "numeric"]):
            # Could be measure or key
            if any(p in name_lower for p in ["_id", "_key", "_code"]):
                return ColumnRole.KEY, 0.7
            return ColumnRole.MEASURE, 0.5

        if any(t in type_lower for t in ["date", "time", "timestamp"]):
            return ColumnRole.DATE, 0.9

        if any(t in type_lower for t in ["bool", "bit"]):
            return ColumnRole.FLAG, 0.9

        if any(t in type_lower for t in ["char", "varchar", "text", "string"]):
            return ColumnRole.ATTRIBUTE, 0.4

        return ColumnRole.UNKNOWN, 0.0

    def _infer_entity_type(self, name: str) -> Tuple[Optional[EntityType], float]:
        """Infer entity type from a name (column or table)."""
        name_lower = name.lower()

        for entity_type, patterns in self.ENTITY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, name_lower, re.IGNORECASE):
                    return entity_type, 0.7

        return None, 0.0

    def _infer_table_entity_type(
        self, table: SourceTable
    ) -> Tuple[Optional[EntityType], float]:
        """Infer entity type for a table."""
        # First check table name
        entity_type, confidence = self._infer_entity_type(table.name)
        if entity_type:
            return entity_type, confidence

        # Check if any column strongly suggests an entity type
        entity_votes: Dict[EntityType, int] = {}
        for col in table.columns:
            if col.entity_type:
                entity_votes[col.entity_type] = entity_votes.get(col.entity_type, 0) + 1

        if entity_votes:
            best_entity = max(entity_votes, key=entity_votes.get)
            confidence = entity_votes[best_entity] / len(table.columns)
            return best_entity, min(confidence, 0.6)

        return None, 0.0

    def _detect_scd_type(self, table: SourceTable) -> Optional[SCDType]:
        """Detect if a table uses SCD Type 2."""
        col_names_lower = [c.name.lower() for c in table.columns]

        # Check for SCD2 indicators
        scd2_matches = sum(
            1 for ind in self.SCD2_INDICATORS
            if any(ind in name for name in col_names_lower)
        )

        if scd2_matches >= 2:
            return SCDType.TYPE_2

        return None

    def _detect_foreign_key(
        self, column_name: str, table_name: str
    ) -> Optional[str]:
        """
        Detect potential foreign key reference from column name.

        Returns reference in format "table.column" or None.
        """
        name_lower = column_name.lower()

        # Pattern: <entity>_id suggests foreign key to <entity> table
        match = re.match(r"^(.+)_id$", name_lower)
        if match:
            entity_name = match.group(1)
            # Don't mark as FK if it's the table's own ID
            if entity_name not in table_name.lower():
                return f"{entity_name}.id"

        # Pattern: fk_<entity> or <entity>_fk
        match = re.match(r"^fk_(.+)$|^(.+)_fk$", name_lower)
        if match:
            entity_name = match.group(1) or match.group(2)
            return f"{entity_name}.id"

        return None

    def _infer_entities(self, tables: List[SourceTable]) -> List[SourceEntity]:
        """Infer business entities from analyzed tables."""
        entities: List[SourceEntity] = []
        entity_map: Dict[EntityType, List[SourceTable]] = {}

        # Group tables by entity type
        for table in tables:
            if table.entity_type:
                if table.entity_type not in entity_map:
                    entity_map[table.entity_type] = []
                entity_map[table.entity_type].append(table)

        # Create entities
        for entity_type, entity_tables in entity_map.items():
            # Use the most confident table as the primary source
            primary_table = max(entity_tables, key=lambda t: t.confidence)

            entity = SourceEntity(
                name=entity_type.value.replace("_", " ").title(),
                entity_type=entity_type,
                description=f"Inferred from {len(entity_tables)} table(s)",
                source_tables=[t.full_path for t in entity_tables],
                key_columns=[
                    f"{t.full_path}.{c.name}"
                    for t in entity_tables
                    for c in t.columns
                    if c.is_primary_key
                ],
                confidence=primary_table.confidence,
                inferred_by="heuristic",
            )
            entities.append(entity)

        return entities

    def _infer_relationships(
        self, tables: List[SourceTable]
    ) -> List[SourceRelationship]:
        """Infer relationships between tables."""
        relationships: List[SourceRelationship] = []

        # Build lookup for table names
        table_names = {t.name.lower(): t for t in tables}

        for table in tables:
            for col in table.columns:
                if col.is_foreign_key and col.foreign_key_reference:
                    # Parse reference
                    parts = col.foreign_key_reference.split(".")
                    if len(parts) >= 1:
                        ref_table_name = parts[0].lower()
                        ref_col_name = parts[1] if len(parts) > 1 else "id"

                        # Check if referenced table exists
                        if ref_table_name in table_names:
                            ref_table = table_names[ref_table_name]

                            rel = SourceRelationship(
                                name=f"{table.name}_to_{ref_table.name}",
                                source_entity=table.full_path,
                                target_entity=ref_table.full_path,
                                relationship_type=RelationshipType.MANY_TO_ONE,
                                source_columns=[col.name],
                                target_columns=[ref_col_name],
                                confidence=0.7,
                                inferred_by="name_match",
                            )
                            relationships.append(rel)

        return relationships

    def analyze_table_only(
        self,
        table_name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        sample_size: int = 100,
    ) -> SourceTable:
        """
        Analyze a single table.

        Useful for adding tables to an existing model.

        Args:
            table_name: Name of the table to analyze.
            database: Database containing the table.
            schema: Schema containing the table.
            sample_size: Number of sample values to retrieve.

        Returns:
            SourceTable with analysis results.
        """
        if not self.adapter:
            raise ValueError("No database adapter configured")

        # Get table info
        tables_info = self.adapter.list_tables(database=database, schema=schema)
        table_info = next((t for t in tables_info if t.name.upper() == table_name.upper()), None)

        if not table_info:
            raise ValueError(f"Table not found: {table_name}")

        return self._analyze_table(
            table_info.name,
            table_info.database,
            table_info.schema,
            table_info.table_type,
            table_info.row_count,
            sample_size,
        )
