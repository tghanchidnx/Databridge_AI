"""
Unit tests for mapping validator.

Tests validation of source mappings against database schemas.
"""

import pytest

from src.validation.mapping_validator import (
    MappingValidator,
    MappingValidationResult,
    MappingIssue,
    MappingIssueType,
    MappingIssueSeverity,
    SchemaInfo,
    get_mapping_validator,
    reset_mapping_validator,
)


class TestMappingValidationResult:
    """Tests for MappingValidationResult dataclass."""

    def test_basic_creation(self):
        """Test creating a basic result."""
        result = MappingValidationResult(project_id="test")

        assert result.project_id == "test"
        assert result.is_valid is True
        assert result.mappings_checked == 0

    def test_add_error_invalidates(self):
        """Test that adding an error makes result invalid."""
        result = MappingValidationResult(project_id="test")

        result.add_issue(MappingIssue(
            issue_type=MappingIssueType.COLUMN_NOT_FOUND,
            severity=MappingIssueSeverity.ERROR,
            message="Column missing",
        ))

        assert result.is_valid is False
        assert result.error_count == 1

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = MappingValidationResult(
            project_id="test",
            mappings_checked=5,
            tables_validated=2,
        )

        data = result.to_dict()

        assert data["project_id"] == "test"
        assert data["mappings_checked"] == 5
        assert "checked_at" in data


class TestMappingIssue:
    """Tests for MappingIssue dataclass."""

    def test_basic_creation(self):
        """Test creating a basic issue."""
        issue = MappingIssue(
            issue_type=MappingIssueType.TABLE_NOT_FOUND,
            severity=MappingIssueSeverity.ERROR,
            message="Table not found",
            hierarchy_id="test-hier",
            mapping_index=0,
        )

        assert issue.issue_type == MappingIssueType.TABLE_NOT_FOUND
        assert issue.mapping_index == 0

    def test_to_dict(self):
        """Test serialization."""
        issue = MappingIssue(
            issue_type=MappingIssueType.COLUMN_NOT_FOUND,
            severity=MappingIssueSeverity.ERROR,
            message="Column missing",
            details={"table": "dim_product"},
        )

        data = issue.to_dict()

        assert data["issue_type"] == "column_not_found"
        assert data["details"]["table"] == "dim_product"


class TestMappingValidator:
    """Tests for MappingValidator class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_mapping_validator()
        self.validator = MappingValidator()

    def test_empty_mappings(self):
        """Test validating empty mappings list."""
        result = self.validator.validate_mappings(
            project_id="empty",
            mappings=[],
        )

        assert result.is_valid is True
        assert result.mappings_checked == 0

    def test_valid_mapping(self):
        """Test validating a valid mapping."""
        mappings = [
            {
                "hierarchy_id": "REVENUE",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "ACCOUNT_CODE",
                "source_uid": "4100",
                "precedence_group": "1",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="valid",
            mappings=mappings,
        )

        # Should have no errors (only possible INFO about source_uid)
        assert result.error_count == 0
        assert result.mappings_checked == 1

    def test_missing_source_table(self):
        """Test detection of missing source_table."""
        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "",
                "source_column": "CODE",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="missing-table",
            mappings=mappings,
        )

        assert result.is_valid is False
        table_issues = [i for i in result.issues if i.issue_type == MappingIssueType.TABLE_NOT_FOUND]
        assert len(table_issues) == 1

    def test_missing_source_column(self):
        """Test detection of missing source_column."""
        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="missing-column",
            mappings=mappings,
        )

        assert result.is_valid is False
        column_issues = [i for i in result.issues if i.issue_type == MappingIssueType.COLUMN_NOT_FOUND]
        assert len(column_issues) == 1

    def test_duplicate_mapping_detection(self):
        """Test detection of duplicate mappings."""
        mappings = [
            {
                "hierarchy_id": "REVENUE",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4100",
            },
            {
                "hierarchy_id": "REVENUE",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4100",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="dup-test",
            mappings=mappings,
        )

        dup_issues = [i for i in result.issues if i.issue_type == MappingIssueType.DUPLICATE_MAPPING]
        assert len(dup_issues) == 1
        assert dup_issues[0].details["duplicate_of_index"] == 0

    def test_empty_source_uid_info(self):
        """Test info message for empty source_uid."""
        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "include_flag": True,
            },
        ]

        result = self.validator.validate_mappings(
            project_id="uid-test",
            mappings=mappings,
        )

        # Should have info about missing source_uid
        uid_issues = [i for i in result.issues if i.issue_type == MappingIssueType.EMPTY_SOURCE_UID]
        assert len(uid_issues) == 1
        assert uid_issues[0].severity == MappingIssueSeverity.INFO

    def test_multiple_precedence_groups_info(self):
        """Test info message for multiple precedence groups."""
        mappings = [
            {
                "hierarchy_id": "REVENUE",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4100",
                "precedence_group": "1",
            },
            {
                "hierarchy_id": "REVENUE",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4200",
                "precedence_group": "2",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="prec-test",
            mappings=mappings,
        )

        prec_issues = [i for i in result.issues if i.issue_type == MappingIssueType.INVALID_PRECEDENCE]
        assert len(prec_issues) == 1
        assert "1" in prec_issues[0].details["precedence_groups"]
        assert "2" in prec_issues[0].details["precedence_groups"]

    def test_table_column_counts(self):
        """Test that unique tables and columns are counted."""
        mappings = [
            {
                "hierarchy_id": "A",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "1",
            },
            {
                "hierarchy_id": "B",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "NAME",
                "source_uid": "2",
            },
            {
                "hierarchy_id": "C",
                "source_database": "DW",
                "source_schema": "SALES",
                "source_table": "DIM_PRODUCT",
                "source_column": "SKU",
                "source_uid": "3",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="count-test",
            mappings=mappings,
        )

        assert result.tables_validated == 2  # DIM_ACCOUNT and DIM_PRODUCT
        assert result.columns_validated == 3


class TestSchemaValidation:
    """Tests for schema-based validation."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_mapping_validator()
        self.validator = MappingValidator()

        # Create mock schema info
        self.schema_info = SchemaInfo(
            database="DW",
            schema="FINANCE",
            tables={
                "DIM_ACCOUNT": {"CODE", "NAME", "TYPE"},
                "FACT_GL": {"AMOUNT", "DATE", "ACCOUNT_ID"},
            },
            column_types={
                "DIM_ACCOUNT.CODE": "VARCHAR",
                "DIM_ACCOUNT.NAME": "VARCHAR",
                "FACT_GL.AMOUNT": "DECIMAL",
            },
        )

    def test_validate_against_schema_table_exists(self):
        """Test validation passes when table exists."""
        def mock_fetcher(db, schema, table):
            return self.schema_info

        self.validator.set_schema_fetcher(mock_fetcher)

        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4100",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="schema-test",
            mappings=mappings,
            validate_against_database=True,
        )

        # Should not have table or column not found errors
        schema_errors = [
            i for i in result.issues
            if i.issue_type in [MappingIssueType.TABLE_NOT_FOUND, MappingIssueType.COLUMN_NOT_FOUND]
            and i.severity == MappingIssueSeverity.ERROR
        ]
        # The required field checks will trigger, but schema validation should pass
        # Filter for only schema-specific issues (those with database details)
        assert result.error_count == 0

    def test_validate_against_schema_table_not_found(self):
        """Test validation fails when table doesn't exist."""
        def mock_fetcher(db, schema, table):
            return self.schema_info

        self.validator.set_schema_fetcher(mock_fetcher)

        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "NONEXISTENT_TABLE",
                "source_column": "CODE",
                "source_uid": "4100",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="table-missing",
            mappings=mappings,
            validate_against_database=True,
        )

        assert result.is_valid is False
        table_issues = [
            i for i in result.issues
            if i.issue_type == MappingIssueType.TABLE_NOT_FOUND
        ]
        assert len(table_issues) == 1

    def test_validate_against_schema_column_not_found(self):
        """Test validation fails when column doesn't exist."""
        def mock_fetcher(db, schema, table):
            return self.schema_info

        self.validator.set_schema_fetcher(mock_fetcher)

        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "NONEXISTENT_COLUMN",
                "source_uid": "4100",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="column-missing",
            mappings=mappings,
            validate_against_database=True,
        )

        assert result.is_valid is False
        column_issues = [
            i for i in result.issues
            if i.issue_type == MappingIssueType.COLUMN_NOT_FOUND
        ]
        assert len(column_issues) == 1
        # Should include available columns
        assert "available_columns" in column_issues[0].details

    def test_schema_fetcher_error_handling(self):
        """Test handling of schema fetcher errors."""
        def mock_fetcher(db, schema, table):
            raise ConnectionError("Database unavailable")

        self.validator.set_schema_fetcher(mock_fetcher)

        mappings = [
            {
                "hierarchy_id": "TEST",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE",
                "source_uid": "4100",
            },
        ]

        result = self.validator.validate_mappings(
            project_id="error-test",
            mappings=mappings,
            validate_against_database=True,
        )

        # Should have connection error warning
        conn_issues = [i for i in result.issues if i.issue_type == MappingIssueType.CONNECTION_ERROR]
        assert len(conn_issues) == 1

    def test_validate_single_mapping(self):
        """Test validating a single mapping."""
        mapping = {
            "hierarchy_id": "TEST",
            "source_database": "DW",
            "source_schema": "FINANCE",
            "source_table": "DIM_ACCOUNT",
            "source_column": "CODE",
            "source_uid": "4100",
        }

        issues = self.validator.validate_single_mapping(
            mapping,
            schema_info=self.schema_info,
        )

        # Should not have errors
        errors = [i for i in issues if i.severity == MappingIssueSeverity.ERROR]
        assert len(errors) == 0


class TestSingletonBehavior:
    """Tests for singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_mapping_validator()

    def test_get_returns_singleton(self):
        """Test that get_mapping_validator returns singleton."""
        v1 = get_mapping_validator()
        v2 = get_mapping_validator()

        assert v1 is v2

    def test_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        v1 = get_mapping_validator()
        reset_mapping_validator()
        v2 = get_mapping_validator()

        assert v1 is not v2

    def test_cache_cleared_on_reset(self):
        """Test that cache is cleared on reset."""
        validator = get_mapping_validator()

        # Populate cache via validation
        validator.validate_mappings("test", [
            {
                "hierarchy_id": "A",
                "source_database": "DW",
                "source_schema": "FINANCE",
                "source_table": "T",
                "source_column": "C",
                "source_uid": "1",
            },
        ])

        reset_mapping_validator()

        # New instance should have empty cache
        new_validator = get_mapping_validator()
        assert len(new_validator._schema_cache) == 0
