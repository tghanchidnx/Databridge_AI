"""
Tests for Phase 25: Data Quality / Expectations Integration.

Tests expectation suites, data contracts, validation runner, and MCP tools.
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

# Import types
from src.data_quality.types import (
    ExpectationType,
    SeverityLevel,
    ValidationStatus,
    Expectation,
    ExpectationSuite,
    ColumnSchema,
    QualityRules,
    SlaConfig,
    DataContract,
    ExpectationResult,
    ValidationResult,
)

# Import generators
from src.data_quality.suite_generator import ExpectationSuiteGenerator
from src.data_quality.contract_generator import DataContractGenerator
from src.data_quality.validation_runner import ValidationRunner


class TestDataQualityTypes:
    """Test data quality types and enums."""

    def test_expectation_type_enum(self):
        """Test ExpectationType enum."""
        assert ExpectationType.NOT_NULL.value == "expect_column_values_to_not_be_null"
        assert ExpectationType.UNIQUE.value == "expect_column_values_to_be_unique"
        assert ExpectationType.IN_SET.value == "expect_column_values_to_be_in_set"

    def test_severity_level_enum(self):
        """Test SeverityLevel enum."""
        assert SeverityLevel.CRITICAL.value == "critical"
        assert SeverityLevel.HIGH.value == "high"
        assert SeverityLevel.MEDIUM.value == "medium"

    def test_validation_status_enum(self):
        """Test ValidationStatus enum."""
        assert ValidationStatus.SUCCESS.value == "success"
        assert ValidationStatus.FAILURE.value == "failure"

    def test_expectation_creation(self):
        """Test Expectation creation."""
        exp = Expectation(
            expectation_type=ExpectationType.NOT_NULL,
            column="account_code",
            severity=SeverityLevel.HIGH,
        )
        assert exp.column == "account_code"
        assert exp.severity == SeverityLevel.HIGH

    def test_expectation_to_ge_format(self):
        """Test converting expectation to GE format."""
        exp = Expectation(
            expectation_type=ExpectationType.MATCH_REGEX,
            column="code",
            kwargs={"regex": "^[0-9]+$"},
        )
        ge_format = exp.to_ge_format()

        assert ge_format["expectation_type"] == "expect_column_values_to_match_regex"
        assert ge_format["kwargs"]["column"] == "code"
        assert ge_format["kwargs"]["regex"] == "^[0-9]+$"

    def test_expectation_suite_creation(self):
        """Test ExpectationSuite creation."""
        suite = ExpectationSuite(
            name="test_suite",
            description="Test suite",
            table_name="test_table",
        )
        assert suite.name == "test_suite"
        assert len(suite.expectations) == 0

    def test_suite_add_expectation(self):
        """Test adding expectation to suite."""
        suite = ExpectationSuite(name="test")
        exp = Expectation(
            expectation_type=ExpectationType.NOT_NULL,
            column="col1",
        )
        suite.add_expectation(exp)

        assert len(suite.expectations) == 1

    def test_column_schema(self):
        """Test ColumnSchema creation."""
        col = ColumnSchema(
            name="account_code",
            data_type="VARCHAR",
            not_null=True,
            unique=True,
            pattern="^[0-9]{4}$",
        )
        assert col.name == "account_code"
        assert col.not_null is True
        assert col.unique is True

    def test_quality_rules(self):
        """Test QualityRules creation."""
        rules = QualityRules(
            freshness_max_age_hours=24,
            completeness_min_percent=99.5,
            uniqueness_columns=["id"],
        )
        assert rules.freshness_max_age_hours == 24
        assert rules.completeness_min_percent == 99.5

    def test_sla_config(self):
        """Test SlaConfig creation."""
        sla = SlaConfig(
            validation_schedule="0 6 * * *",
            alert_on_failure=True,
            alert_channels=["slack://alerts"],
        )
        assert sla.validation_schedule == "0 6 * * *"
        assert sla.alert_on_failure is True

    def test_data_contract(self):
        """Test DataContract creation."""
        contract = DataContract(
            name="test_contract",
            version="1.0.0",
            owner="data-team",
        )
        assert contract.name == "test_contract"
        assert contract.version == "1.0.0"

    def test_validation_result(self):
        """Test ValidationResult creation."""
        result = ValidationResult(
            suite_name="test",
            status=ValidationStatus.SUCCESS,
            success_count=5,
            failure_count=0,
            total_expectations=5,
        )
        assert result.success_percent == 100.0


class TestExpectationSuiteGenerator:
    """Test expectation suite generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = ExpectationSuiteGenerator(output_dir=self.temp_dir)

    def test_create_suite(self):
        """Test creating a suite."""
        suite = self.generator.create_suite(
            name="test_suite",
            description="Test",
            table_name="test_table",
        )
        assert suite.name == "test_suite"

    def test_create_duplicate_suite_fails(self):
        """Test that duplicate suite fails."""
        self.generator.create_suite(name="test")

        with pytest.raises(ValueError, match="already exists"):
            self.generator.create_suite(name="test")

    def test_get_suite(self):
        """Test getting a suite."""
        created = self.generator.create_suite(name="mytest")
        retrieved = self.generator.get_suite("mytest")

        assert retrieved is not None
        assert retrieved.id == created.id

    def test_list_suites(self):
        """Test listing suites."""
        self.generator.create_suite(name="suite1")
        self.generator.create_suite(name="suite2")

        suites = self.generator.list_suites()
        assert len(suites) == 2

    def test_add_expectation(self):
        """Test adding expectation to suite."""
        self.generator.create_suite(name="test")
        exp = self.generator.add_expectation(
            suite_name="test",
            expectation_type=ExpectationType.NOT_NULL,
            column="col1",
        )

        suite = self.generator.get_suite("test")
        assert len(suite.expectations) == 1

    def test_generate_from_hierarchy(self):
        """Test generating from hierarchy mappings."""
        self.generator.create_suite(name="hierarchy_test")

        mappings = [
            {
                "source_column": "ACCOUNT_CODE",
                "source_uid": "4%",
                "source_database": "DB",
                "source_schema": "SCHEMA",
                "source_table": "TABLE",
            },
            {
                "source_column": "ACCOUNT_CODE",
                "source_uid": "5%",
                "source_database": "DB",
                "source_schema": "SCHEMA",
                "source_table": "TABLE",
            },
        ]

        suite = self.generator.generate_from_hierarchy(
            suite_name="hierarchy_test",
            hierarchy_nodes=[],
            mappings=mappings,
        )

        # Should have expectations for ACCOUNT_CODE
        assert len(suite.expectations) > 0

    def test_export_suite_json(self):
        """Test exporting suite to JSON."""
        suite = self.generator.create_suite(name="export_test")
        self.generator.add_expectation(
            suite_name="export_test",
            expectation_type=ExpectationType.NOT_NULL,
            column="col1",
        )

        json_str = self.generator.export_suite("export_test", format="json")
        data = json.loads(json_str)

        assert data["expectation_suite_name"] == "export_test"
        assert len(data["expectations"]) == 1


class TestDataContractGenerator:
    """Test data contract generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = DataContractGenerator(output_dir=self.temp_dir)

    def test_create_contract(self):
        """Test creating a contract."""
        contract = self.generator.create_contract(
            name="test_contract",
            owner="data-team",
            table_name="test_table",
        )
        assert contract.name == "test_contract"
        assert contract.owner == "data-team"

    def test_add_column(self):
        """Test adding column to contract."""
        self.generator.create_contract(name="test")
        col = self.generator.add_column(
            contract_name="test",
            name="account_code",
            data_type="VARCHAR",
            not_null=True,
        )

        contract = self.generator.get_contract("test")
        assert len(contract.columns) == 1
        assert contract.columns[0].not_null is True

    def test_set_quality_rules(self):
        """Test setting quality rules."""
        self.generator.create_contract(name="test")
        rules = self.generator.set_quality_rules(
            contract_name="test",
            freshness_max_age_hours=24,
            completeness_min_percent=99.0,
        )

        assert rules.freshness_max_age_hours == 24
        assert rules.completeness_min_percent == 99.0

    def test_set_sla(self):
        """Test setting SLA."""
        self.generator.create_contract(name="test")
        sla = self.generator.set_sla(
            contract_name="test",
            validation_schedule="0 6 * * *",
            alert_channels=["slack://test"],
        )

        assert sla.validation_schedule == "0 6 * * *"
        assert "slack://test" in sla.alert_channels

    def test_export_contract_yaml(self):
        """Test exporting contract to YAML."""
        self.generator.create_contract(name="test", owner="team")
        self.generator.add_column(
            contract_name="test",
            name="col1",
            data_type="VARCHAR",
            not_null=True,
        )

        yaml_str = self.generator.export_contract("test", format="yaml")

        assert "name: test" in yaml_str
        assert "owner: team" in yaml_str
        assert "col1" in yaml_str

    def test_export_to_file(self):
        """Test exporting to file."""
        self.generator.create_contract(name="file_test")

        file_path = self.generator.export_to_file("file_test")
        assert Path(file_path).exists()


class TestValidationRunner:
    """Test validation runner."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.runner = ValidationRunner(output_dir=self.temp_dir)

    def test_validate_not_null(self):
        """Test validating not null expectation."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.NOT_NULL,
            column="col1",
        ))

        data = [
            {"col1": "a"},
            {"col1": "b"},
            {"col1": "c"},
        ]

        result = self.runner.validate_dataframe(suite, data)

        assert result.status == ValidationStatus.SUCCESS
        assert result.success_count == 1

    def test_validate_not_null_fails(self):
        """Test that not null fails with nulls."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.NOT_NULL,
            column="col1",
        ))

        data = [
            {"col1": "a"},
            {"col1": None},
            {"col1": "c"},
        ]

        result = self.runner.validate_dataframe(suite, data)

        assert result.status == ValidationStatus.FAILURE
        assert result.failure_count == 1

    def test_validate_unique(self):
        """Test validating unique expectation."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.UNIQUE,
            column="id",
        ))

        data = [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.SUCCESS

    def test_validate_unique_fails(self):
        """Test that unique fails with duplicates."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.UNIQUE,
            column="id",
        ))

        data = [
            {"id": 1},
            {"id": 1},
            {"id": 2},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.FAILURE

    def test_validate_in_set(self):
        """Test validating in_set expectation."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.IN_SET,
            column="status",
            kwargs={"value_set": ["active", "inactive"]},
        ))

        data = [
            {"status": "active"},
            {"status": "inactive"},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.SUCCESS

    def test_validate_in_set_fails(self):
        """Test that in_set fails with unexpected values."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.IN_SET,
            column="status",
            kwargs={"value_set": ["active", "inactive"]},
        ))

        data = [
            {"status": "active"},
            {"status": "unknown"},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.FAILURE

    def test_validate_regex(self):
        """Test validating regex expectation."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.MATCH_REGEX,
            column="code",
            kwargs={"regex": "^[0-9]{4}$"},
        ))

        data = [
            {"code": "1234"},
            {"code": "5678"},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.SUCCESS

    def test_validate_regex_fails(self):
        """Test that regex fails with non-matching values."""
        suite = ExpectationSuite(name="test")
        suite.add_expectation(Expectation(
            expectation_type=ExpectationType.MATCH_REGEX,
            column="code",
            kwargs={"regex": "^[0-9]{4}$"},
        ))

        data = [
            {"code": "1234"},
            {"code": "abc"},
        ]

        result = self.runner.validate_dataframe(suite, data)
        assert result.status == ValidationStatus.FAILURE

    def test_validate_contract(self):
        """Test validating a data contract."""
        contract = DataContract(
            name="test",
            columns=[
                ColumnSchema(name="id", data_type="NUMBER", not_null=True, unique=True),
                ColumnSchema(name="name", data_type="VARCHAR", not_null=True),
            ],
        )

        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        result = self.runner.validate_contract(contract, data)
        assert result.status == ValidationStatus.SUCCESS

    def test_generate_report_markdown(self):
        """Test generating markdown report."""
        result = ValidationResult(
            suite_name="test",
            status=ValidationStatus.SUCCESS,
            success_count=3,
            failure_count=0,
            total_expectations=3,
            row_count=100,
        )

        report = self.runner.generate_report(result, format="markdown")

        assert "# Validation Report: test" in report
        assert "SUCCESS" in report
        assert "100.0%" in report


class TestMCPTools:
    """Test MCP tools registration."""

    def test_register_data_quality_tools(self):
        """Test registering data quality tools."""
        from src.data_quality.mcp_tools import register_data_quality_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        result = register_data_quality_tools(mock_mcp)

        assert result["tools_registered"] == 7
        assert "generate_expectation_suite" in result["tools"]
        assert "add_column_expectation" in result["tools"]
        assert "create_data_contract" in result["tools"]
        assert "run_validation" in result["tools"]
        assert "get_validation_results" in result["tools"]
        assert "list_expectation_suites" in result["tools"]
        assert "export_data_contract" in result["tools"]


class TestModuleExports:
    """Test module exports."""

    def test_all_exports(self):
        """Test that all expected items are exported."""
        from src.data_quality import (
            # Types
            ExpectationType,
            SeverityLevel,
            ValidationStatus,
            Expectation,
            ExpectationSuite,
            ColumnSchema,
            QualityRules,
            SlaConfig,
            DataContract,
            ExpectationResult,
            ValidationResult,
            # Generators
            ExpectationSuiteGenerator,
            DataContractGenerator,
            ValidationRunner,
            # MCP
            register_data_quality_tools,
        )

        # Just verify imports work
        assert ExpectationType.NOT_NULL is not None
        assert SeverityLevel.HIGH is not None
        assert ValidationStatus.SUCCESS is not None
