"""
Schema Scanner Agent for extracting database metadata.

Capabilities:
- scan_schema: Scan database schema for tables and columns
- extract_metadata: Extract detailed metadata
- detect_keys: Detect primary/foreign keys
- sample_profiles: Profile data samples
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import re

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentCapability,
    AgentConfig,
    AgentResult,
    AgentError,
    TaskContext,
)


@dataclass
class TableMetadata:
    """Metadata for a database table."""

    name: str
    schema: str = ""
    database: str = ""
    columns: list[dict[str, Any]] = field(default_factory=list)
    row_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    comment: str | None = None
    table_type: str = "TABLE"  # TABLE, VIEW, MATERIALIZED_VIEW

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "schema": self.schema,
            "database": self.database,
            "columns": self.columns,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "comment": self.comment,
            "table_type": self.table_type,
        }


@dataclass
class ColumnMetadata:
    """Metadata for a database column."""

    name: str
    data_type: str
    nullable: bool = True
    default_value: str | None = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: str | None = None
    comment: str | None = None
    ordinal_position: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "default_value": self.default_value,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "foreign_key_ref": self.foreign_key_ref,
            "comment": self.comment,
            "ordinal_position": self.ordinal_position,
        }


@dataclass
class DataProfile:
    """Data profiling results for a column."""

    column_name: str
    distinct_count: int = 0
    null_count: int = 0
    min_value: Any = None
    max_value: Any = None
    avg_value: float | None = None
    sample_values: list[Any] = field(default_factory=list)
    pattern_detected: str | None = None
    data_quality_score: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "distinct_count": self.distinct_count,
            "null_count": self.null_count,
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "avg_value": self.avg_value,
            "sample_values": [str(v) for v in self.sample_values[:10]],
            "pattern_detected": self.pattern_detected,
            "data_quality_score": self.data_quality_score,
        }


class SchemaScanner(BaseAgent):
    """
    Schema Scanner Agent for extracting database metadata.

    Scans database schemas to extract:
    - Table and column definitions
    - Primary/foreign key relationships
    - Data profiles and statistics
    - Table relationships

    Example:
        scanner = SchemaScanner()

        context = TaskContext(
            task_id="scan_1",
            input_data={
                "schema_definition": {...},  # Or connection info
            }
        )

        result = scanner.execute(
            AgentCapability.SCAN_SCHEMA,
            context
        )
    """

    # Common key name patterns
    KEY_PATTERNS = {
        "primary_key": [
            r'^id$',
            r'.*_id$',
            r'^pk_.*',
            r'.*_pk$',
            r'^key$',
        ],
        "foreign_key": [
            r'.*_id$',
            r'^fk_.*',
            r'.*_fk$',
            r'.*_ref$',
        ],
    }

    # Data type classifications
    TYPE_CATEGORIES = {
        "string": ["varchar", "char", "text", "string", "nvarchar", "nchar"],
        "integer": ["int", "integer", "bigint", "smallint", "tinyint"],
        "decimal": ["decimal", "numeric", "float", "double", "real", "number"],
        "boolean": ["boolean", "bool", "bit"],
        "date": ["date"],
        "timestamp": ["timestamp", "datetime", "datetime2"],
        "json": ["json", "jsonb", "variant", "object"],
    }

    def __init__(self, config: AgentConfig | None = None):
        """Initialize Schema Scanner."""
        super().__init__(config or AgentConfig(name="SchemaScanner"))
        self._scanned_schemas: dict[str, list[TableMetadata]] = {}
        self._profiles: dict[str, list[DataProfile]] = {}

    def get_capabilities(self) -> list[AgentCapability]:
        """Get supported capabilities."""
        return [
            AgentCapability.SCAN_SCHEMA,
            AgentCapability.EXTRACT_METADATA,
            AgentCapability.DETECT_KEYS,
            AgentCapability.SAMPLE_PROFILES,
        ]

    def execute(
        self,
        capability: AgentCapability,
        context: TaskContext,
        **kwargs: Any,
    ) -> AgentResult:
        """
        Execute a capability.

        Args:
            capability: The capability to execute
            context: Task context with input data
            **kwargs: Additional arguments

        Returns:
            AgentResult with execution results
        """
        if not self.supports(capability):
            raise AgentError(
                f"Capability {capability} not supported",
                self.name,
                capability.value,
            )

        start_time = self._start_execution(capability, context)

        try:
            if capability == AgentCapability.SCAN_SCHEMA:
                data = self._scan_schema(context, **kwargs)
            elif capability == AgentCapability.EXTRACT_METADATA:
                data = self._extract_metadata(context, **kwargs)
            elif capability == AgentCapability.DETECT_KEYS:
                data = self._detect_keys(context, **kwargs)
            elif capability == AgentCapability.SAMPLE_PROFILES:
                data = self._sample_profiles(context, **kwargs)
            else:
                raise AgentError(f"Unknown capability: {capability}", self.name)

            return self._complete_execution(capability, start_time, True, data)

        except AgentError as e:
            self._handle_error(e)
            return self._complete_execution(capability, start_time, False, error=str(e))
        except Exception as e:
            error = AgentError(str(e), self.name, capability.value)
            self._handle_error(error)
            return self._complete_execution(capability, start_time, False, error=str(e))

    def _scan_schema(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Scan database schema for tables and columns.

        Input data:
            - schema_definition: Dict with tables/columns (for offline mode)
            - connection: Connection info (for live mode)
            - schema_name: Schema to scan
        """
        self._report_progress("Starting schema scan", 0.0)

        input_data = context.input_data
        tables: list[TableMetadata] = []

        # Handle schema definition (offline mode)
        if "schema_definition" in input_data:
            schema_def = input_data["schema_definition"]
            tables = self._parse_schema_definition(schema_def)

        # Handle table list input
        elif "tables" in input_data:
            for table_data in input_data["tables"]:
                table = TableMetadata(
                    name=table_data.get("name", ""),
                    schema=table_data.get("schema", input_data.get("schema_name", "")),
                    database=table_data.get("database", input_data.get("database_name", "")),
                    columns=[
                        ColumnMetadata(
                            name=c.get("name", ""),
                            data_type=c.get("data_type", "VARCHAR"),
                            nullable=c.get("nullable", True),
                        ).to_dict()
                        for c in table_data.get("columns", [])
                    ],
                    row_count=table_data.get("row_count"),
                    table_type=table_data.get("table_type", "TABLE"),
                )
                tables.append(table)

        # Handle SQL DDL input
        elif "ddl" in input_data:
            tables = self._parse_ddl(input_data["ddl"])

        self._report_progress("Schema scan complete", 1.0)

        # Store results
        schema_key = input_data.get("schema_name", "default")
        self._scanned_schemas[schema_key] = tables

        return {
            "schema_name": schema_key,
            "table_count": len(tables),
            "tables": [t.to_dict() for t in tables],
            "total_columns": sum(len(t.columns) for t in tables),
        }

    def _extract_metadata(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Extract detailed metadata from schema.

        Input data:
            - tables: List of TableMetadata dicts
            - include_statistics: Whether to include stats
        """
        self._report_progress("Extracting metadata", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])

        metadata = {
            "tables": [],
            "relationships": [],
            "statistics": {},
        }

        for i, table_data in enumerate(tables):
            self._report_progress(f"Processing {table_data.get('name', 'table')}", i / len(tables))

            # Classify columns
            columns = table_data.get("columns", [])
            classified = self._classify_columns(columns)

            metadata["tables"].append({
                "name": table_data.get("name"),
                "schema": table_data.get("schema"),
                "columns": classified,
                "column_count": len(columns),
                "estimated_type": self._infer_table_type(table_data.get("name", ""), columns),
            })

        # Detect relationships
        if len(tables) > 1:
            metadata["relationships"] = self._detect_relationships(tables)

        self._report_progress("Metadata extraction complete", 1.0)

        return metadata

    def _detect_keys(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Detect primary and foreign keys.

        Input data:
            - tables: List of table metadata
            - use_naming_conventions: Use naming patterns
        """
        self._report_progress("Detecting keys", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])
        use_naming = input_data.get("use_naming_conventions", True)

        key_results = {
            "primary_keys": [],
            "foreign_keys": [],
            "candidate_keys": [],
        }

        for table_data in tables:
            table_name = table_data.get("name", "")
            columns = table_data.get("columns", [])

            for col in columns:
                col_name = col.get("name", "")

                # Check for primary key
                if col.get("is_primary_key"):
                    key_results["primary_keys"].append({
                        "table": table_name,
                        "column": col_name,
                        "confidence": 1.0,
                        "source": "explicit",
                    })
                elif use_naming and self._matches_pattern(col_name, "primary_key"):
                    key_results["candidate_keys"].append({
                        "table": table_name,
                        "column": col_name,
                        "key_type": "primary_key",
                        "confidence": 0.8,
                        "source": "naming_convention",
                    })

                # Check for foreign key
                if col.get("is_foreign_key"):
                    key_results["foreign_keys"].append({
                        "table": table_name,
                        "column": col_name,
                        "references": col.get("foreign_key_ref"),
                        "confidence": 1.0,
                        "source": "explicit",
                    })
                elif use_naming and self._matches_pattern(col_name, "foreign_key"):
                    # Try to infer reference table
                    ref_table = self._infer_reference_table(col_name, tables)
                    key_results["candidate_keys"].append({
                        "table": table_name,
                        "column": col_name,
                        "key_type": "foreign_key",
                        "inferred_reference": ref_table,
                        "confidence": 0.7 if ref_table else 0.5,
                        "source": "naming_convention",
                    })

        self._report_progress("Key detection complete", 1.0)

        return key_results

    def _sample_profiles(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Profile data samples from tables.

        Input data:
            - tables: List of table metadata with sample data
            - sample_size: Number of rows to profile
        """
        self._report_progress("Profiling data", 0.0)

        input_data = context.input_data
        tables = input_data.get("tables", [])
        sample_size = input_data.get("sample_size", 100)

        profiles = []

        for table_data in tables:
            table_name = table_data.get("name", "")
            columns = table_data.get("columns", [])
            sample_data = table_data.get("sample_data", [])

            table_profiles = []

            for col in columns:
                col_name = col.get("name", "")

                # Extract column values from sample
                values = [
                    row.get(col_name) for row in sample_data
                    if col_name in row
                ]

                profile = self._profile_column(col_name, col.get("data_type", ""), values)
                table_profiles.append(profile.to_dict())

            profiles.append({
                "table": table_name,
                "sample_size": len(sample_data),
                "column_profiles": table_profiles,
            })

        # Store profiles
        self._profiles = {p["table"]: p["column_profiles"] for p in profiles}

        self._report_progress("Profiling complete", 1.0)

        return {
            "profile_count": len(profiles),
            "profiles": profiles,
        }

    def _parse_schema_definition(self, schema_def: dict[str, Any]) -> list[TableMetadata]:
        """Parse schema definition dictionary into TableMetadata objects."""
        tables = []

        for table_name, table_info in schema_def.get("tables", {}).items():
            columns = []
            for col_name, col_info in table_info.get("columns", {}).items():
                if isinstance(col_info, str):
                    # Simple format: column_name: data_type
                    columns.append(ColumnMetadata(
                        name=col_name,
                        data_type=col_info,
                    ).to_dict())
                else:
                    # Detailed format
                    columns.append(ColumnMetadata(
                        name=col_name,
                        data_type=col_info.get("type", "VARCHAR"),
                        nullable=col_info.get("nullable", True),
                        is_primary_key=col_info.get("primary_key", False),
                        is_foreign_key=col_info.get("foreign_key", False),
                        foreign_key_ref=col_info.get("references"),
                    ).to_dict())

            tables.append(TableMetadata(
                name=table_name,
                schema=schema_def.get("schema", ""),
                database=schema_def.get("database", ""),
                columns=columns,
            ))

        return tables

    def _parse_ddl(self, ddl: str) -> list[TableMetadata]:
        """Parse SQL DDL to extract table metadata."""
        tables = []

        # Simple DDL parsing
        create_pattern = r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)\s*\((.*?)\);'
        matches = re.findall(create_pattern, ddl, re.IGNORECASE | re.DOTALL)

        for table_name, columns_str in matches:
            # Clean table name
            table_name = table_name.strip('`"[]').split('.')[-1]

            columns = []
            # Parse columns
            col_lines = columns_str.split(',')
            for line in col_lines:
                line = line.strip()
                if not line or line.upper().startswith(('PRIMARY', 'FOREIGN', 'UNIQUE', 'INDEX', 'KEY', 'CONSTRAINT')):
                    continue

                parts = line.split()
                if len(parts) >= 2:
                    col_name = parts[0].strip('`"[]')
                    col_type = parts[1].upper()

                    columns.append(ColumnMetadata(
                        name=col_name,
                        data_type=col_type,
                        nullable='NOT NULL' not in line.upper(),
                        is_primary_key='PRIMARY KEY' in line.upper(),
                    ).to_dict())

            tables.append(TableMetadata(
                name=table_name,
                columns=columns,
            ))

        return tables

    def _classify_columns(self, columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Classify columns by type and purpose."""
        classified = []

        for col in columns:
            col_name = col.get("name", "").lower()
            data_type = col.get("data_type", "").lower()

            # Determine category
            category = "unknown"
            for cat, types in self.TYPE_CATEGORIES.items():
                if any(t in data_type for t in types):
                    category = cat
                    break

            # Determine purpose
            purpose = "attribute"
            if col.get("is_primary_key") or self._matches_pattern(col_name, "primary_key"):
                purpose = "key"
            elif col.get("is_foreign_key") or self._matches_pattern(col_name, "foreign_key"):
                purpose = "foreign_key"
            elif any(t in col_name for t in ["created", "updated", "modified", "_at", "_date"]):
                purpose = "audit"
            elif any(t in col_name for t in ["amount", "qty", "count", "total", "sum"]):
                purpose = "measure"
            elif any(t in col_name for t in ["flag", "is_", "has_"]):
                purpose = "flag"

            classified.append({
                **col,
                "category": category,
                "purpose": purpose,
            })

        return classified

    def _detect_relationships(self, tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Detect relationships between tables."""
        relationships = []
        table_names = {t.get("name", "").lower(): t for t in tables}

        for table in tables:
            table_name = table.get("name", "")
            columns = table.get("columns", [])

            for col in columns:
                col_name = col.get("name", "").lower()

                # Check if column name suggests a foreign key
                if col_name.endswith("_id") and col_name != "id":
                    # Extract potential reference table name
                    ref_name = col_name[:-3]  # Remove _id

                    # Check if reference table exists
                    if ref_name in table_names or ref_name + "s" in table_names:
                        ref_table = ref_name if ref_name in table_names else ref_name + "s"
                        relationships.append({
                            "from_table": table_name,
                            "from_column": col.get("name"),
                            "to_table": table_names[ref_table].get("name"),
                            "to_column": "id",
                            "relationship_type": "many_to_one",
                            "confidence": 0.75,
                        })

        return relationships

    def _matches_pattern(self, name: str, pattern_type: str) -> bool:
        """Check if name matches key patterns."""
        patterns = self.KEY_PATTERNS.get(pattern_type, [])
        name_lower = name.lower()

        for pattern in patterns:
            if re.match(pattern, name_lower):
                return True
        return False

    def _infer_reference_table(
        self,
        column_name: str,
        tables: list[dict[str, Any]],
    ) -> str | None:
        """Infer reference table from column name."""
        # Extract potential table name from column
        col_lower = column_name.lower()

        if col_lower.endswith("_id"):
            potential_table = col_lower[:-3]

            # Check against existing tables
            table_names = [t.get("name", "").lower() for t in tables]

            if potential_table in table_names:
                return potential_table
            if potential_table + "s" in table_names:
                return potential_table + "s"

        return None

    def _infer_table_type(self, table_name: str, columns: list[dict[str, Any]]) -> str:
        """Infer table type (fact, dimension, staging, etc.)."""
        name_lower = table_name.lower()

        # Check prefixes
        if name_lower.startswith(("dim_", "d_")):
            return "dimension"
        if name_lower.startswith(("fact_", "fct_", "f_")):
            return "fact"
        if name_lower.startswith(("stg_", "staging_")):
            return "staging"
        if name_lower.startswith(("raw_", "src_")):
            return "raw"

        # Check column patterns
        col_names = [c.get("name", "").lower() for c in columns]

        # Fact tables typically have many foreign keys and measures
        fk_count = sum(1 for c in col_names if c.endswith("_id"))
        measure_count = sum(1 for c in col_names if any(
            m in c for m in ["amount", "qty", "quantity", "total", "sum", "count"]
        ))

        if fk_count >= 3 and measure_count >= 1:
            return "fact"

        # Dimension tables typically have description/name columns
        if any("name" in c or "description" in c or "desc" in c for c in col_names):
            return "dimension"

        return "table"

    def _profile_column(
        self,
        column_name: str,
        data_type: str,
        values: list[Any],
    ) -> DataProfile:
        """Profile a single column."""
        if not values:
            return DataProfile(column_name=column_name)

        # Filter out None values for calculations
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)

        profile = DataProfile(
            column_name=column_name,
            distinct_count=len(set(str(v) for v in non_null)),
            null_count=null_count,
            sample_values=non_null[:10],
        )

        # Calculate stats for numeric types
        if non_null and all(isinstance(v, (int, float)) for v in non_null):
            profile.min_value = min(non_null)
            profile.max_value = max(non_null)
            profile.avg_value = sum(non_null) / len(non_null)

        # Detect patterns for strings
        if non_null and all(isinstance(v, str) for v in non_null):
            profile.pattern_detected = self._detect_string_pattern(non_null)

        # Calculate quality score
        profile.data_quality_score = 1.0 - (null_count / len(values) if values else 0)

        return profile

    def _detect_string_pattern(self, values: list[str]) -> str | None:
        """Detect common patterns in string values."""
        if not values:
            return None

        # Check for common patterns
        patterns = {
            "email": r'^[\w\.-]+@[\w\.-]+\.\w+$',
            "uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            "date": r'^\d{4}-\d{2}-\d{2}$',
            "phone": r'^\+?\d{10,15}$',
            "code": r'^[A-Z0-9]{2,10}$',
        }

        for pattern_name, pattern in patterns.items():
            if all(re.match(pattern, str(v), re.IGNORECASE) for v in values[:20]):
                return pattern_name

        return None
