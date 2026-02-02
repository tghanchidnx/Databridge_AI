"""
Source Mapping Validation for DataBridge AI Librarian.

Validates source mappings against database schemas:
- Column existence checks
- Data type compatibility
- Table accessibility
- Precedence group consistency
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Set, Callable


class MappingIssueType(str, Enum):
    """Types of mapping validation issues."""

    TABLE_NOT_FOUND = "table_not_found"
    COLUMN_NOT_FOUND = "column_not_found"
    SCHEMA_NOT_FOUND = "schema_not_found"
    DATABASE_NOT_FOUND = "database_not_found"
    TYPE_MISMATCH = "type_mismatch"
    DUPLICATE_MAPPING = "duplicate_mapping"
    INVALID_PRECEDENCE = "invalid_precedence"
    CONNECTION_ERROR = "connection_error"
    EMPTY_SOURCE_UID = "empty_source_uid"


class MappingIssueSeverity(str, Enum):
    """Severity levels for mapping issues."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class MappingIssue:
    """Represents a mapping validation issue."""

    issue_type: MappingIssueType
    severity: MappingIssueSeverity
    message: str
    hierarchy_id: Optional[str] = None
    mapping_index: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "issue_type": self.issue_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "hierarchy_id": self.hierarchy_id,
            "mapping_index": self.mapping_index,
            "details": self.details,
        }


@dataclass
class MappingValidationResult:
    """Result of mapping validation."""

    project_id: str
    is_valid: bool = True
    issues: List[MappingIssue] = field(default_factory=list)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    mappings_checked: int = 0
    tables_validated: int = 0
    columns_validated: int = 0

    @property
    def error_count(self) -> int:
        """Count of ERROR severity issues."""
        return sum(1 for i in self.issues if i.severity == MappingIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        """Count of WARNING severity issues."""
        return sum(1 for i in self.issues if i.severity == MappingIssueSeverity.WARNING)

    def add_issue(self, issue: MappingIssue) -> None:
        """Add an issue to the result."""
        self.issues.append(issue)
        if issue.severity == MappingIssueSeverity.ERROR:
            self.is_valid = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "project_id": self.project_id,
            "is_valid": self.is_valid,
            "checked_at": self.checked_at.isoformat(),
            "mappings_checked": self.mappings_checked,
            "tables_validated": self.tables_validated,
            "columns_validated": self.columns_validated,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [i.to_dict() for i in self.issues],
        }


@dataclass
class SchemaInfo:
    """Cached schema information for validation."""

    database: str
    schema: str
    tables: Dict[str, Set[str]]  # table_name -> set of column names
    column_types: Dict[str, Dict[str, str]]  # table.column -> type


class MappingValidator:
    """
    Validates source mappings against database schemas.

    Features:
    - Table and column existence checks
    - Data type compatibility validation
    - Duplicate mapping detection
    - Precedence group validation
    - Schema caching for performance
    """

    def __init__(self):
        """Initialize the validator."""
        self._schema_cache: Dict[str, SchemaInfo] = {}
        self._schema_fetcher: Optional[Callable] = None

    def set_schema_fetcher(
        self,
        fetcher: Callable[[str, str, str], Optional[SchemaInfo]],
    ) -> None:
        """
        Set a function to fetch schema information.

        The fetcher should accept (database, schema, table) and return
        SchemaInfo or None if not accessible.

        Args:
            fetcher: Function to fetch schema information.
        """
        self._schema_fetcher = fetcher

    def validate_mappings(
        self,
        project_id: str,
        mappings: List[Dict[str, Any]],
        validate_against_database: bool = False,
    ) -> MappingValidationResult:
        """
        Validate a list of source mappings.

        Args:
            project_id: The project ID being validated.
            mappings: List of mapping dictionaries.
            validate_against_database: Whether to check against actual database.

        Returns:
            MappingValidationResult with found issues.
        """
        result = MappingValidationResult(
            project_id=project_id,
            mappings_checked=len(mappings),
        )

        if not mappings:
            return result

        # Track for duplicate detection
        seen_mappings: Dict[str, List[int]] = {}
        precedence_groups: Dict[str, Set[str]] = {}  # hierarchy_id -> set of groups

        for i, mapping in enumerate(mappings):
            hierarchy_id = mapping.get("hierarchy_id")
            source_db = mapping.get("source_database", "")
            source_schema = mapping.get("source_schema", "")
            source_table = mapping.get("source_table", "")
            source_column = mapping.get("source_column", "")
            source_uid = mapping.get("source_uid", "")
            precedence = mapping.get("precedence_group", "1")

            # Check required fields
            self._check_required_fields(result, mapping, i)

            # Build key for duplicate detection
            mapping_key = f"{hierarchy_id}:{source_db}.{source_schema}.{source_table}.{source_column}:{source_uid}"

            if mapping_key in seen_mappings:
                result.add_issue(MappingIssue(
                    issue_type=MappingIssueType.DUPLICATE_MAPPING,
                    severity=MappingIssueSeverity.WARNING,
                    message=f"Duplicate mapping for hierarchy '{hierarchy_id}'",
                    hierarchy_id=hierarchy_id,
                    mapping_index=i,
                    details={
                        "duplicate_of_index": seen_mappings[mapping_key][0],
                        "source_table": source_table,
                        "source_column": source_column,
                    },
                ))
                seen_mappings[mapping_key].append(i)
            else:
                seen_mappings[mapping_key] = [i]

            # Track precedence groups
            if hierarchy_id:
                if hierarchy_id not in precedence_groups:
                    precedence_groups[hierarchy_id] = set()
                precedence_groups[hierarchy_id].add(str(precedence))

            # Validate against database if enabled
            if validate_against_database and self._schema_fetcher:
                self._validate_against_schema(result, mapping, i)

        # Check precedence consistency
        self._check_precedence_consistency(result, precedence_groups)

        # Count unique tables/columns validated
        unique_tables = set()
        unique_columns = set()
        for mapping in mappings:
            table_key = f"{mapping.get('source_database')}.{mapping.get('source_schema')}.{mapping.get('source_table')}"
            unique_tables.add(table_key)
            unique_columns.add(f"{table_key}.{mapping.get('source_column')}")

        result.tables_validated = len(unique_tables)
        result.columns_validated = len(unique_columns)

        return result

    def _check_required_fields(
        self,
        result: MappingValidationResult,
        mapping: Dict[str, Any],
        index: int,
    ) -> None:
        """Check that required fields are present."""
        hierarchy_id = mapping.get("hierarchy_id")

        if not mapping.get("source_table"):
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.TABLE_NOT_FOUND,
                severity=MappingIssueSeverity.ERROR,
                message="Mapping missing source_table",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
            ))

        if not mapping.get("source_column"):
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.COLUMN_NOT_FOUND,
                severity=MappingIssueSeverity.ERROR,
                message="Mapping missing source_column",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
            ))

        # Check source_uid for leaf mappings
        include_flag = mapping.get("include_flag", True)
        if include_flag and not mapping.get("source_uid"):
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.EMPTY_SOURCE_UID,
                severity=MappingIssueSeverity.INFO,
                message="Mapping has no source_uid filter - will match all values",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
            ))

    def _validate_against_schema(
        self,
        result: MappingValidationResult,
        mapping: Dict[str, Any],
        index: int,
    ) -> None:
        """Validate mapping against actual database schema."""
        hierarchy_id = mapping.get("hierarchy_id")
        source_db = mapping.get("source_database", "")
        source_schema = mapping.get("source_schema", "")
        source_table = mapping.get("source_table", "")
        source_column = mapping.get("source_column", "")

        if not all([source_db, source_schema, source_table]):
            return

        # Get schema info
        cache_key = f"{source_db}.{source_schema}"

        if cache_key not in self._schema_cache:
            try:
                schema_info = self._schema_fetcher(source_db, source_schema, source_table)
                if schema_info:
                    self._schema_cache[cache_key] = schema_info
            except Exception as e:
                result.add_issue(MappingIssue(
                    issue_type=MappingIssueType.CONNECTION_ERROR,
                    severity=MappingIssueSeverity.WARNING,
                    message=f"Could not connect to validate schema: {str(e)}",
                    hierarchy_id=hierarchy_id,
                    mapping_index=index,
                    details={"database": source_db, "schema": source_schema},
                ))
                return

        schema_info = self._schema_cache.get(cache_key)
        if not schema_info:
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.SCHEMA_NOT_FOUND,
                severity=MappingIssueSeverity.WARNING,
                message=f"Could not access schema '{source_schema}' in database '{source_db}'",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
            ))
            return

        # Check table exists
        if source_table.upper() not in {t.upper() for t in schema_info.tables}:
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.TABLE_NOT_FOUND,
                severity=MappingIssueSeverity.ERROR,
                message=f"Table '{source_table}' not found in schema '{source_schema}'",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
                details={"database": source_db, "schema": source_schema},
            ))
            return

        # Check column exists
        table_columns = schema_info.tables.get(source_table.upper(), set())
        if source_column.upper() not in {c.upper() for c in table_columns}:
            result.add_issue(MappingIssue(
                issue_type=MappingIssueType.COLUMN_NOT_FOUND,
                severity=MappingIssueSeverity.ERROR,
                message=f"Column '{source_column}' not found in table '{source_table}'",
                hierarchy_id=hierarchy_id,
                mapping_index=index,
                details={
                    "database": source_db,
                    "schema": source_schema,
                    "table": source_table,
                    "available_columns": list(table_columns)[:10],  # Show first 10
                },
            ))

    def _check_precedence_consistency(
        self,
        result: MappingValidationResult,
        precedence_groups: Dict[str, Set[str]],
    ) -> None:
        """Check precedence group consistency across hierarchies."""
        # Find hierarchies with multiple precedence groups
        for hierarchy_id, groups in precedence_groups.items():
            if len(groups) > 1:
                result.add_issue(MappingIssue(
                    issue_type=MappingIssueType.INVALID_PRECEDENCE,
                    severity=MappingIssueSeverity.INFO,
                    message=f"Hierarchy '{hierarchy_id}' has mappings in multiple precedence groups",
                    hierarchy_id=hierarchy_id,
                    details={"precedence_groups": list(groups)},
                ))

    def validate_single_mapping(
        self,
        mapping: Dict[str, Any],
        schema_info: Optional[SchemaInfo] = None,
    ) -> List[MappingIssue]:
        """
        Validate a single mapping.

        Args:
            mapping: The mapping to validate.
            schema_info: Optional schema info for database validation.

        Returns:
            List of validation issues.
        """
        result = MappingValidationResult(project_id="single")

        self._check_required_fields(result, mapping, 0)

        if schema_info:
            self._schema_cache["temp"] = schema_info
            self._validate_against_schema(result, mapping, 0)
            del self._schema_cache["temp"]

        return result.issues

    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._schema_cache.clear()


# Singleton instance
_mapping_validator_instance: Optional[MappingValidator] = None


def get_mapping_validator() -> MappingValidator:
    """Get the singleton MappingValidator instance."""
    global _mapping_validator_instance
    if _mapping_validator_instance is None:
        _mapping_validator_instance = MappingValidator()
    return _mapping_validator_instance


def reset_mapping_validator() -> None:
    """Reset the singleton MappingValidator instance."""
    global _mapping_validator_instance
    _mapping_validator_instance = None
