"""
Data Dictionary Generator for automated documentation.

This module generates data dictionaries from discovered hierarchies:
- Column definitions
- Table definitions
- Relationship documentation
- Export to various formats (JSON, CSV, Markdown)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from databridge_discovery.hierarchy.case_to_hierarchy import ConvertedHierarchy


class DataType(str, Enum):
    """Standard data types for dictionary."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    JSON = "json"
    ARRAY = "array"


class ColumnCategory(str, Enum):
    """Column categories."""

    KEY = "key"
    ATTRIBUTE = "attribute"
    MEASURE = "measure"
    FLAG = "flag"
    AUDIT = "audit"
    HIERARCHY = "hierarchy"


@dataclass
class ColumnDefinition:
    """Definition of a column in the data dictionary."""

    name: str
    data_type: DataType
    description: str
    nullable: bool = True
    category: ColumnCategory = ColumnCategory.ATTRIBUTE
    example_value: str | None = None
    business_name: str | None = None
    format_pattern: str | None = None
    allowed_values: list[str] | None = None
    source_column: str | None = None
    transformation: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "description": self.description,
            "nullable": self.nullable,
            "category": self.category.value,
            "example_value": self.example_value,
            "business_name": self.business_name,
            "format_pattern": self.format_pattern,
            "allowed_values": self.allowed_values,
            "source_column": self.source_column,
            "transformation": self.transformation,
            "notes": self.notes,
        }


@dataclass
class TableDefinition:
    """Definition of a table in the data dictionary."""

    name: str
    description: str
    columns: list[ColumnDefinition]
    schema_name: str = ""
    business_name: str | None = None
    owner: str | None = None
    update_frequency: str | None = None
    primary_key: list[str] | None = None
    foreign_keys: list[dict[str, str]] | None = None
    row_count_estimate: int | None = None
    tags: list[str] = field(default_factory=list)
    notes: str | None = None

    @property
    def column_count(self) -> int:
        return len(self.columns)

    def get_column(self, name: str) -> ColumnDefinition | None:
        """Get column by name."""
        for col in self.columns:
            if col.name.upper() == name.upper():
                return col
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "schema_name": self.schema_name,
            "description": self.description,
            "business_name": self.business_name,
            "owner": self.owner,
            "update_frequency": self.update_frequency,
            "primary_key": self.primary_key,
            "foreign_keys": self.foreign_keys,
            "row_count_estimate": self.row_count_estimate,
            "column_count": self.column_count,
            "tags": self.tags,
            "notes": self.notes,
            "columns": [col.to_dict() for col in self.columns],
        }


@dataclass
class DataDictionary:
    """Complete data dictionary."""

    name: str
    tables: list[TableDefinition]
    description: str = ""
    version: str = "1.0"
    generated_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def table_count(self) -> int:
        return len(self.tables)

    @property
    def total_columns(self) -> int:
        return sum(t.column_count for t in self.tables)

    def get_table(self, name: str) -> TableDefinition | None:
        """Get table by name."""
        for table in self.tables:
            if table.name.upper() == name.upper():
                return table
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "generated_at": self.generated_at.isoformat(),
            "table_count": self.table_count,
            "total_columns": self.total_columns,
            "metadata": self.metadata,
            "tables": [t.to_dict() for t in self.tables],
        }


class DataDictionaryGenerator:
    """
    Generates data dictionaries from discovered hierarchies.

    Creates comprehensive documentation including:
    - Table and column definitions
    - Business names and descriptions
    - Data types and constraints
    - Example values
    - Relationships

    Example:
        generator = DataDictionaryGenerator()

        # Generate from hierarchies
        dictionary = generator.generate(
            hierarchies=[hierarchy1, hierarchy2],
            name="GL_HIERARCHIES"
        )

        # Export to markdown
        markdown = generator.to_markdown(dictionary)

        # Export to JSON
        json_data = dictionary.to_dict()
    """

    # Standard hierarchy columns
    STANDARD_COLUMNS = [
        ColumnDefinition(
            name="HIERARCHY_ID",
            data_type=DataType.STRING,
            description="Unique identifier for the hierarchy node",
            nullable=False,
            category=ColumnCategory.KEY,
            example_value="REVENUE_001",
            business_name="Hierarchy Node ID",
        ),
        ColumnDefinition(
            name="HIERARCHY_NAME",
            data_type=DataType.STRING,
            description="Display name for the hierarchy node",
            nullable=True,
            category=ColumnCategory.ATTRIBUTE,
            example_value="Total Revenue",
            business_name="Node Name",
        ),
        ColumnDefinition(
            name="PARENT_ID",
            data_type=DataType.STRING,
            description="Reference to parent node in the hierarchy",
            nullable=True,
            category=ColumnCategory.KEY,
            example_value="INCOME_ROOT",
            business_name="Parent Node ID",
        ),
        ColumnDefinition(
            name="DESCRIPTION",
            data_type=DataType.STRING,
            description="Detailed description of the hierarchy node",
            nullable=True,
            category=ColumnCategory.ATTRIBUTE,
            example_value="All revenue line items",
            business_name="Description",
        ),
        ColumnDefinition(
            name="INCLUDE_FLAG",
            data_type=DataType.BOOLEAN,
            description="Flag indicating if node is included in rollups",
            nullable=True,
            category=ColumnCategory.FLAG,
            example_value="TRUE",
            business_name="Include Flag",
            allowed_values=["TRUE", "FALSE"],
        ),
        ColumnDefinition(
            name="EXCLUDE_FLAG",
            data_type=DataType.BOOLEAN,
            description="Flag indicating if node is excluded from output",
            nullable=True,
            category=ColumnCategory.FLAG,
            example_value="FALSE",
            business_name="Exclude Flag",
            allowed_values=["TRUE", "FALSE"],
        ),
        ColumnDefinition(
            name="FORMULA_GROUP",
            data_type=DataType.STRING,
            description="Formula group for calculations (SUM, SUBTRACT, etc.)",
            nullable=True,
            category=ColumnCategory.ATTRIBUTE,
            example_value="SUM",
            business_name="Formula Group",
            allowed_values=["SUM", "SUBTRACT", "MULTIPLY", "DIVIDE", "NONE"],
        ),
        ColumnDefinition(
            name="SORT_ORDER",
            data_type=DataType.INTEGER,
            description="Order for display and reporting",
            nullable=True,
            category=ColumnCategory.ATTRIBUTE,
            example_value="100",
            business_name="Sort Order",
        ),
        ColumnDefinition(
            name="CREATED_AT",
            data_type=DataType.TIMESTAMP,
            description="Timestamp when record was created",
            nullable=True,
            category=ColumnCategory.AUDIT,
            example_value="2024-01-15 10:30:00",
            business_name="Created Timestamp",
        ),
        ColumnDefinition(
            name="UPDATED_AT",
            data_type=DataType.TIMESTAMP,
            description="Timestamp when record was last updated",
            nullable=True,
            category=ColumnCategory.AUDIT,
            example_value="2024-01-15 14:45:00",
            business_name="Updated Timestamp",
        ),
    ]

    def __init__(self):
        """Initialize the generator."""
        pass

    def generate(
        self,
        hierarchies: list[ConvertedHierarchy],
        name: str,
        description: str = "",
    ) -> DataDictionary:
        """
        Generate a data dictionary from hierarchies.

        Args:
            hierarchies: List of hierarchies to document
            name: Name for the dictionary
            description: Description of the dictionary

        Returns:
            Complete DataDictionary
        """
        tables = []

        for hier in hierarchies:
            # Generate TBL_0 definition
            tbl_0 = self._generate_table_definition(hier)
            tables.append(tbl_0)

            # Generate mapping table definition if has mappings
            if hier.nodes:
                mapping_table = self._generate_mapping_table_definition(hier)
                tables.append(mapping_table)

        return DataDictionary(
            name=name,
            tables=tables,
            description=description or f"Data dictionary for {len(hierarchies)} hierarchy(s)",
            metadata={
                "hierarchy_count": len(hierarchies),
                "entity_types": list(set(h.entity_type for h in hierarchies)),
            },
        )

    def generate_for_table(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        schema_name: str = "",
    ) -> TableDefinition:
        """
        Generate table definition from column metadata.

        Args:
            table_name: Name of the table
            columns: Column metadata dictionaries
            schema_name: Schema name

        Returns:
            TableDefinition
        """
        col_defs = []

        for col in columns:
            col_def = ColumnDefinition(
                name=col.get("name", ""),
                data_type=self._infer_data_type(col.get("data_type", "string")),
                description=col.get("description", ""),
                nullable=col.get("nullable", True),
                category=self._infer_category(col.get("name", "")),
                example_value=col.get("example"),
                business_name=col.get("business_name"),
            )
            col_defs.append(col_def)

        return TableDefinition(
            name=table_name,
            schema_name=schema_name,
            description=f"Table {table_name}",
            columns=col_defs,
        )

    def _generate_table_definition(
        self,
        hierarchy: ConvertedHierarchy,
    ) -> TableDefinition:
        """Generate table definition for a hierarchy."""
        table_name = self._sanitize_name(f"TBL_0_{hierarchy.name}")
        columns = list(self.STANDARD_COLUMNS)

        # Add level columns
        for i in range(1, min(hierarchy.level_count + 2, 11)):
            columns.append(ColumnDefinition(
                name=f"LEVEL_{i}",
                data_type=DataType.STRING,
                description=f"Value at hierarchy level {i}",
                nullable=True,
                category=ColumnCategory.HIERARCHY,
                example_value=f"Level {i} Value",
                business_name=f"Level {i}",
            ))
            columns.append(ColumnDefinition(
                name=f"LEVEL_{i}_SORT",
                data_type=DataType.INTEGER,
                description=f"Sort order within level {i}",
                nullable=True,
                category=ColumnCategory.HIERARCHY,
                example_value=str(i * 100),
                business_name=f"Level {i} Sort Order",
            ))

        return TableDefinition(
            name=table_name,
            schema_name="HIERARCHIES",
            description=f"Hierarchy table for {hierarchy.name}",
            columns=columns,
            business_name=hierarchy.name,
            primary_key=["HIERARCHY_ID"],
            tags=[hierarchy.entity_type, "hierarchy", "tbl_0"],
            notes=f"Entity type: {hierarchy.entity_type}, Levels: {hierarchy.level_count}",
        )

    def _generate_mapping_table_definition(
        self,
        hierarchy: ConvertedHierarchy,
    ) -> TableDefinition:
        """Generate mapping table definition."""
        table_name = self._sanitize_name(f"TBL_0_{hierarchy.name}_MAPPING")

        columns = [
            ColumnDefinition(
                name="HIERARCHY_ID",
                data_type=DataType.STRING,
                description="Foreign key to hierarchy table",
                nullable=False,
                category=ColumnCategory.KEY,
            ),
            ColumnDefinition(
                name="MAPPING_INDEX",
                data_type=DataType.INTEGER,
                description="Order of mapping within hierarchy node",
                nullable=False,
                category=ColumnCategory.KEY,
            ),
            ColumnDefinition(
                name="SOURCE_DATABASE",
                data_type=DataType.STRING,
                description="Source database name",
                nullable=True,
                category=ColumnCategory.ATTRIBUTE,
            ),
            ColumnDefinition(
                name="SOURCE_SCHEMA",
                data_type=DataType.STRING,
                description="Source schema name",
                nullable=True,
                category=ColumnCategory.ATTRIBUTE,
            ),
            ColumnDefinition(
                name="SOURCE_TABLE",
                data_type=DataType.STRING,
                description="Source table name",
                nullable=True,
                category=ColumnCategory.ATTRIBUTE,
            ),
            ColumnDefinition(
                name="SOURCE_COLUMN",
                data_type=DataType.STRING,
                description="Source column name",
                nullable=True,
                category=ColumnCategory.ATTRIBUTE,
            ),
            ColumnDefinition(
                name="SOURCE_UID",
                data_type=DataType.STRING,
                description="Specific source value to match",
                nullable=True,
                category=ColumnCategory.KEY,
            ),
            ColumnDefinition(
                name="PRECEDENCE_GROUP",
                data_type=DataType.INTEGER,
                description="Precedence for conflict resolution",
                nullable=True,
                category=ColumnCategory.ATTRIBUTE,
            ),
        ]

        return TableDefinition(
            name=table_name,
            schema_name="HIERARCHIES",
            description=f"Source mappings for {hierarchy.name} hierarchy",
            columns=columns,
            primary_key=["HIERARCHY_ID", "MAPPING_INDEX"],
            foreign_keys=[{
                "column": "HIERARCHY_ID",
                "references": f"TBL_0_{self._sanitize_name(hierarchy.name)}(HIERARCHY_ID)",
            }],
            tags=[hierarchy.entity_type, "mapping"],
        )

    def to_markdown(self, dictionary: DataDictionary) -> str:
        """
        Export data dictionary to Markdown format.

        Args:
            dictionary: Dictionary to export

        Returns:
            Markdown string
        """
        lines = [
            f"# {dictionary.name}",
            "",
            dictionary.description,
            "",
            f"**Version:** {dictionary.version}",
            f"**Generated:** {dictionary.generated_at.strftime('%Y-%m-%d %H:%M')}",
            f"**Tables:** {dictionary.table_count}",
            f"**Total Columns:** {dictionary.total_columns}",
            "",
            "---",
            "",
            "## Tables",
            "",
        ]

        for table in dictionary.tables:
            lines.append(f"### {table.name}")
            lines.append("")
            lines.append(table.description)
            lines.append("")

            if table.business_name:
                lines.append(f"**Business Name:** {table.business_name}")
            if table.primary_key:
                lines.append(f"**Primary Key:** {', '.join(table.primary_key)}")
            if table.tags:
                lines.append(f"**Tags:** {', '.join(table.tags)}")
            lines.append("")

            # Column table
            lines.append("| Column | Type | Nullable | Description |")
            lines.append("|--------|------|----------|-------------|")

            for col in table.columns:
                nullable = "Yes" if col.nullable else "No"
                lines.append(f"| {col.name} | {col.data_type.value} | {nullable} | {col.description} |")

            lines.append("")

        return "\n".join(lines)

    def to_csv(self, dictionary: DataDictionary) -> str:
        """
        Export data dictionary to CSV format.

        Args:
            dictionary: Dictionary to export

        Returns:
            CSV string
        """
        lines = [
            "Table,Column,DataType,Nullable,Category,Description,BusinessName,Example"
        ]

        for table in dictionary.tables:
            for col in table.columns:
                nullable = "Y" if col.nullable else "N"
                example = col.example_value or ""
                business = col.business_name or ""
                lines.append(
                    f'"{table.name}","{col.name}","{col.data_type.value}","{nullable}",'
                    f'"{col.category.value}","{col.description}","{business}","{example}"'
                )

        return "\n".join(lines)

    def _infer_data_type(self, type_str: str) -> DataType:
        """Infer DataType from string."""
        type_lower = type_str.lower()

        if any(t in type_lower for t in ["int", "number", "bigint"]):
            return DataType.INTEGER
        elif any(t in type_lower for t in ["decimal", "float", "double", "numeric"]):
            return DataType.DECIMAL
        elif any(t in type_lower for t in ["bool", "bit"]):
            return DataType.BOOLEAN
        elif "timestamp" in type_lower or "datetime" in type_lower:
            return DataType.TIMESTAMP
        elif "date" in type_lower:
            return DataType.DATE
        elif "json" in type_lower or "variant" in type_lower:
            return DataType.JSON
        elif "array" in type_lower:
            return DataType.ARRAY
        else:
            return DataType.STRING

    def _infer_category(self, column_name: str) -> ColumnCategory:
        """Infer column category from name."""
        name_upper = column_name.upper()

        if name_upper.endswith("_ID") or name_upper == "ID":
            return ColumnCategory.KEY
        elif name_upper.endswith("_FLAG") or name_upper.startswith("IS_"):
            return ColumnCategory.FLAG
        elif any(t in name_upper for t in ["_AT", "CREATED", "UPDATED", "MODIFIED"]):
            return ColumnCategory.AUDIT
        elif name_upper.startswith("LEVEL_"):
            return ColumnCategory.HIERARCHY
        elif any(t in name_upper for t in ["AMOUNT", "COUNT", "SUM", "AVG", "TOTAL"]):
            return ColumnCategory.MEASURE
        else:
            return ColumnCategory.ATTRIBUTE

    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for identifiers."""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        sanitized = re.sub(r'_+', '_', sanitized)
        return sanitized.strip('_').upper()
