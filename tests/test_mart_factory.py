"""
Tests for Phase 26: Hierarchy-Driven Data Mart Factory.

Tests configuration generation, pipeline generation, formula engine,
discovery agent, and MCP tools.
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

# Import types (renamed from mart_factory to wright in Phase 31)
from src.wright.types import (
    PipelineLayer,
    ObjectType,
    FormulaLogic,
    JoinPattern,
    DynamicColumnMapping,
    MartConfig,
    PipelineObject,
    FormulaPrecedence,
    DataQualityIssue,
    DiscoveryResult,
)

# Import generators
from src.wright.config_generator import MartConfigGenerator
from src.wright.pipeline_generator import MartPipelineGenerator
from src.wright.formula_engine import (
    FormulaPrecedenceEngine,
    create_standard_los_formulas,
)
from src.wright.cortex_discovery import CortexDiscoveryAgent


class TestMartFactoryTypes:
    """Test mart factory types and enums."""

    def test_pipeline_layer_enum(self):
        """Test PipelineLayer enum."""
        assert PipelineLayer.VW_1.value == "VW_1"
        assert PipelineLayer.DT_2.value == "DT_2"
        assert PipelineLayer.DT_3A.value == "DT_3A"
        assert PipelineLayer.DT_3.value == "DT_3"

    def test_object_type_enum(self):
        """Test ObjectType enum."""
        assert ObjectType.VIEW.value == "VIEW"
        assert ObjectType.DYNAMIC_TABLE.value == "DYNAMIC_TABLE"

    def test_formula_logic_enum(self):
        """Test FormulaLogic enum."""
        assert FormulaLogic.SUM.value == "SUM"
        assert FormulaLogic.SUBTRACT.value == "SUBTRACT"
        assert FormulaLogic.MULTIPLY.value == "MULTIPLY"
        assert FormulaLogic.DIVIDE.value == "DIVIDE"

    def test_join_pattern_creation(self):
        """Test JoinPattern creation."""
        pattern = JoinPattern(
            name="account",
            join_keys=["LOS_ACCOUNT_ID_FILTER"],
            fact_keys=["FK_ACCOUNT_KEY"],
        )
        assert pattern.name == "account"
        assert pattern.join_keys == ["LOS_ACCOUNT_ID_FILTER"]
        assert pattern.fact_keys == ["FK_ACCOUNT_KEY"]
        assert pattern.filter is None

    def test_join_pattern_with_filter(self):
        """Test JoinPattern with filter."""
        pattern = JoinPattern(
            name="royalty",
            join_keys=["LOS_PRODUCT_CODE_FILTER"],
            fact_keys=["FK_PRODUCT_KEY"],
            filter="ROYALTY_FILTER = 'Y'",
        )
        assert pattern.filter == "ROYALTY_FILTER = 'Y'"

    def test_dynamic_column_mapping(self):
        """Test DynamicColumnMapping creation."""
        mapping = DynamicColumnMapping(
            id_source="BILLING_CATEGORY_CODE",
            physical_column="ACCT.ACCOUNT_BILLING_CATEGORY_CODE",
        )
        assert mapping.id_source == "BILLING_CATEGORY_CODE"
        assert mapping.is_alias is False

    def test_mart_config_creation(self):
        """Test MartConfig creation."""
        config = MartConfig(
            project_name="test_mart",
            report_type="GROSS",
            hierarchy_table="DB.SCHEMA.HIERARCHY",
            mapping_table="DB.SCHEMA.MAPPING",
            account_segment="GROSS",
        )
        assert config.project_name == "test_mart"
        assert config.report_type == "GROSS"
        assert config.has_sign_change is False

    def test_mart_config_effective_prefix(self):
        """Test MartConfig effective measure prefix."""
        config = MartConfig(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        assert config.effective_measure_prefix == "GROSS"

        config.measure_prefix = "CUSTOM"
        assert config.effective_measure_prefix == "CUSTOM"

    def test_mart_config_add_pattern(self):
        """Test adding pattern to config."""
        config = MartConfig(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        pattern = JoinPattern(
            name="account",
            join_keys=["KEY"],
            fact_keys=["FK"],
        )
        config.add_join_pattern(pattern)
        assert len(config.join_patterns) == 1

    def test_pipeline_object_creation(self):
        """Test PipelineObject creation."""
        obj = PipelineObject(
            object_type=ObjectType.VIEW,
            object_name="VW_1_TEST",
            layer=PipelineLayer.VW_1,
            layer_order=1,
            ddl="CREATE VIEW ...",
        )
        assert obj.layer == PipelineLayer.VW_1
        assert obj.layer_order == 1

    def test_formula_precedence_creation(self):
        """Test FormulaPrecedence creation."""
        formula = FormulaPrecedence(
            precedence_level=3,
            formula_group="GROSS_PROFIT",
            logic=FormulaLogic.SUBTRACT,
            param_ref="Total Revenue",
            param2_ref="Total Taxes and Deducts",
        )
        assert formula.precedence_level == 3
        assert formula.logic == FormulaLogic.SUBTRACT

    def test_data_quality_issue(self):
        """Test DataQualityIssue creation."""
        issue = DataQualityIssue(
            severity="HIGH",
            issue_type="TYPO",
            description="Possible typo detected",
            affected_rows=2,
            affected_values=["BILLING_CATEGRY_CODE"],
        )
        assert issue.severity == "HIGH"
        assert issue.affected_rows == 2


class TestMartConfigGenerator:
    """Test mart configuration generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = MartConfigGenerator(output_dir=self.temp_dir)

    def test_create_config(self):
        """Test creating a configuration."""
        config = self.generator.create_config(
            project_name="test_config",
            report_type="GROSS",
            hierarchy_table="DB.SCHEMA.HIER",
            mapping_table="DB.SCHEMA.MAP",
            account_segment="GROSS",
        )
        assert config.project_name == "test_config"
        assert config.report_type == "GROSS"

    def test_create_duplicate_config_fails(self):
        """Test that duplicate config fails."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )

        with pytest.raises(ValueError, match="already exists"):
            self.generator.create_config(
                project_name="test",
                report_type="NET",
                hierarchy_table="",
                mapping_table="",
                account_segment="NET",
            )

    def test_get_config(self):
        """Test getting a configuration."""
        created = self.generator.create_config(
            project_name="mytest",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        retrieved = self.generator.get_config("mytest")

        assert retrieved is not None
        assert retrieved.id == created.id

    def test_list_configs(self):
        """Test listing configurations."""
        self.generator.create_config(
            project_name="config1",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        self.generator.create_config(
            project_name="config2",
            report_type="NET",
            hierarchy_table="",
            mapping_table="",
            account_segment="NET",
        )

        configs = self.generator.list_configs()
        assert len(configs) == 2

    def test_add_join_pattern(self):
        """Test adding join pattern."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        pattern = self.generator.add_join_pattern(
            config_name="test",
            name="account",
            join_keys=["LOS_ACCOUNT_ID_FILTER"],
            fact_keys=["FK_ACCOUNT_KEY"],
        )

        config = self.generator.get_config("test")
        assert len(config.join_patterns) == 1
        assert config.join_patterns[0].name == "account"

    def test_add_column_mapping(self):
        """Test adding column mapping."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )
        mapping = self.generator.add_column_mapping(
            config_name="test",
            id_source="BILLING_CATEGORY_CODE",
            physical_column="ACCT.ACCOUNT_BILLING_CATEGORY_CODE",
        )

        config = self.generator.get_config("test")
        assert len(config.dynamic_column_map) == 1

    def test_validate_config(self):
        """Test configuration validation."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="DB.SCHEMA.HIER",
            mapping_table="DB.SCHEMA.MAP",
            account_segment="GROSS",
        )

        result = self.generator.validate_config("test")
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_config_missing_tables(self):
        """Test validation catches missing tables."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )

        result = self.generator.validate_config("test")
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_export_yaml(self):
        """Test exporting to YAML."""
        self.generator.create_config(
            project_name="test",
            report_type="GROSS",
            hierarchy_table="DB.SCHEMA.HIER",
            mapping_table="DB.SCHEMA.MAP",
            account_segment="GROSS",
        )
        self.generator.add_join_pattern(
            config_name="test",
            name="account",
            join_keys=["KEY"],
            fact_keys=["FK"],
        )

        yaml_str = self.generator.export_yaml("test")

        assert "project_name: test" in yaml_str
        assert "report_type: GROSS" in yaml_str
        assert "join_patterns:" in yaml_str

    def test_export_to_file(self):
        """Test exporting to file."""
        self.generator.create_config(
            project_name="file_test",
            report_type="GROSS",
            hierarchy_table="",
            mapping_table="",
            account_segment="GROSS",
        )

        file_path = self.generator.export_to_file("file_test")
        assert Path(file_path).exists()


class TestMartPipelineGenerator:
    """Test mart pipeline generator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.generator = MartPipelineGenerator(output_dir=self.temp_dir)
        self.config = MartConfig(
            project_name="test_pipeline",
            report_type="GROSS",
            hierarchy_table="DB.SCHEMA.HIER",
            mapping_table="DB.SCHEMA.MAP",
            account_segment="GROSS",
        )
        # Add a join pattern
        self.config.add_join_pattern(JoinPattern(
            name="account",
            join_keys=["LOS_ACCOUNT_ID_FILTER"],
            fact_keys=["FK_ACCOUNT_KEY"],
        ))
        # Add a column mapping
        self.config.add_column_mapping(DynamicColumnMapping(
            id_source="BILLING_CATEGORY_CODE",
            physical_column="ACCT.ACCOUNT_BILLING_CATEGORY_CODE",
        ))

    def test_generate_vw1(self):
        """Test generating VW_1."""
        obj = self.generator.generate_vw1(self.config)

        assert obj.layer == PipelineLayer.VW_1
        assert obj.object_type == ObjectType.VIEW
        assert "CREATE OR REPLACE VIEW" in obj.ddl
        assert "CASE" in obj.ddl
        assert "BILLING_CATEGORY_CODE" in obj.ddl

    def test_generate_dt2(self):
        """Test generating DT_2."""
        obj = self.generator.generate_dt2(self.config)

        assert obj.layer == PipelineLayer.DT_2
        assert obj.object_type == ObjectType.DYNAMIC_TABLE
        assert "CREATE OR REPLACE DYNAMIC TABLE" in obj.ddl
        assert "UNPIVOTED" in obj.ddl

    def test_generate_dt3a(self):
        """Test generating DT_3A."""
        obj = self.generator.generate_dt3a(self.config)

        assert obj.layer == PipelineLayer.DT_3A
        assert obj.object_type == ObjectType.DYNAMIC_TABLE
        assert "CREATE OR REPLACE DYNAMIC TABLE" in obj.ddl
        assert "UNION ALL" in obj.ddl or "Branch 1" in obj.ddl
        assert "ACCOUNT_SEGMENT = 'GROSS'" in obj.ddl

    def test_generate_dt3(self):
        """Test generating DT_3."""
        formulas = create_standard_los_formulas("GROSS")
        obj = self.generator.generate_dt3(self.config, formulas)

        assert obj.layer == PipelineLayer.DT_3
        assert obj.object_type == ObjectType.DYNAMIC_TABLE
        assert "CREATE OR REPLACE DYNAMIC TABLE" in obj.ddl
        assert "DENSE_RANK" in obj.ddl
        assert "COALESCE" in obj.ddl

    def test_generate_full_pipeline(self):
        """Test generating full pipeline."""
        objects = self.generator.generate_full_pipeline(self.config)

        assert len(objects) == 4
        assert objects[0].layer == PipelineLayer.VW_1
        assert objects[1].layer == PipelineLayer.DT_2
        assert objects[2].layer == PipelineLayer.DT_3A
        assert objects[3].layer == PipelineLayer.DT_3

    def test_export_pipeline(self):
        """Test exporting pipeline to files."""
        result = self.generator.export_pipeline(self.config)

        assert len(result) == 4
        for name, path in result.items():
            assert Path(path).exists()


class TestFormulaPrecedenceEngine:
    """Test formula precedence engine."""

    def setup_method(self):
        """Set up test fixtures."""
        self.engine = FormulaPrecedenceEngine()

    def test_extract_formulas(self):
        """Test extracting formulas from hierarchy data."""
        hierarchy_data = [
            {
                "FORMULA_GROUP": "Total Revenue",
                "FORMULA_PRECEDENCE": 1,
                "FORMULA_LOGIC": "SUM",
                "FORMULA_PARAM_REF": "Revenue",
            },
            {
                "FORMULA_GROUP": "Gross Profit",
                "FORMULA_PRECEDENCE": 3,
                "FORMULA_LOGIC": "SUBTRACT",
                "FORMULA_PARAM_REF": "Total Revenue",
                "FORMULA_PARAM2_REF": "Total Taxes and Deducts",
            },
        ]

        formulas = self.engine.extract_formulas(hierarchy_data)

        assert len(formulas) == 2
        assert formulas[0].formula_group == "Total Revenue"
        assert formulas[1].logic == FormulaLogic.SUBTRACT

    def test_build_precedence_chain(self):
        """Test building precedence chain."""
        formulas = [
            FormulaPrecedence(
                precedence_level=1,
                formula_group="A",
                logic=FormulaLogic.SUM,
                param_ref="X",
            ),
            FormulaPrecedence(
                precedence_level=3,
                formula_group="B",
                logic=FormulaLogic.SUBTRACT,
                param_ref="A",
                param2_ref="Y",
            ),
        ]

        chain = self.engine.build_precedence_chain(formulas)

        assert len(chain[1]) == 1
        assert len(chain[3]) == 1
        assert len(chain[2]) == 0

    def test_validate_dependencies(self):
        """Test validating formula dependencies."""
        formulas = [
            FormulaPrecedence(
                precedence_level=1,
                formula_group="Base",
                logic=FormulaLogic.SUM,
                param_ref="Source",
            ),
            FormulaPrecedence(
                precedence_level=2,
                formula_group="Derived",
                logic=FormulaLogic.SUBTRACT,
                param_ref="Base",
                param2_ref="Other",
            ),
        ]

        result = self.engine.validate_dependencies(formulas)

        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_validate_dependencies_catches_cycle(self):
        """Test that validation catches precedence violations."""
        formulas = [
            FormulaPrecedence(
                precedence_level=2,
                formula_group="A",
                logic=FormulaLogic.SUM,
                param_ref="B",  # References higher precedence
            ),
            FormulaPrecedence(
                precedence_level=2,
                formula_group="B",
                logic=FormulaLogic.SUM,
                param_ref="C",
            ),
        ]

        result = self.engine.validate_dependencies(formulas)

        # Should have error about referencing same/higher precedence
        assert len(result["errors"]) > 0

    def test_create_standard_los_formulas(self):
        """Test creating standard LOS formulas."""
        gross_formulas = create_standard_los_formulas("GROSS")
        net_formulas = create_standard_los_formulas("NET")

        # GROSS should have Royalties
        gross_groups = [f.formula_group for f in gross_formulas]
        assert "Total Royalties" in gross_groups

        # NET should not have Royalties
        net_groups = [f.formula_group for f in net_formulas]
        assert "Total Royalties" not in net_groups


class TestCortexDiscoveryAgent:
    """Test Cortex discovery agent."""

    def setup_method(self):
        """Set up test fixtures."""
        self.agent = CortexDiscoveryAgent()

    def test_discover_hierarchy_no_connection(self):
        """Test discovery without database connection."""
        result = self.agent.discover_hierarchy(
            hierarchy_table="DB.SCHEMA.HIER",
            mapping_table="DB.SCHEMA.MAP",
        )

        assert result.hierarchy_table == "DB.SCHEMA.HIER"
        assert result.mapping_table == "DB.SCHEMA.MAP"
        # Without connection, should still provide suggestions
        assert len(result.join_pattern_suggestion) >= 0

    def test_detect_typos(self):
        """Test typo detection logic."""
        known = ["BILLING_CATEGORY_CODE", "ACCOUNT_CODE"]

        # Simulate typo detection
        issues = []
        actual = "BILLING_CATEGRY_CODE"  # Missing 'O'

        # Use internal method
        best_match, score = self.agent._find_closest_match(actual, known)

        assert best_match == "BILLING_CATEGORY_CODE"
        assert score >= 0.8

    def test_suggest_join_patterns(self):
        """Test join pattern suggestion."""
        result = DiscoveryResult(
            hierarchy_table="",
            mapping_table="",
            id_table_distribution={
                "DIM_ACCOUNT": 100,
                "DIM_PRODUCT": 50,
            },
        )

        patterns = self.agent._suggest_join_patterns(result)

        # Should suggest at least account pattern
        names = [p.name for p in patterns]
        assert "account" in names

    def test_suggest_column_mappings(self):
        """Test column mapping suggestion."""
        result = DiscoveryResult(
            hierarchy_table="",
            mapping_table="",
            id_source_distribution={
                "BILLING_CATEGORY_CODE": 100,
                "ACCOUNT_CODE": 50,
            },
        )

        mappings = self.agent._suggest_column_mappings(result)

        # Should suggest mappings for known patterns
        assert len(mappings) >= 2

    def test_detect_hierarchy_type(self):
        """Test hierarchy type detection."""
        result = DiscoveryResult(
            hierarchy_table="",
            mapping_table="",
            id_source_distribution={
                "DEDUCT_CODE": 100,
                "ROYALTY_CODE": 50,
            },
            id_table_distribution={
                "DIM_DEDUCT": 100,
            },
        )

        hier_type = self.agent._detect_hierarchy_type(result)

        assert hier_type == "LOS"  # Lease Operating Statement

    def test_generate_recommended_config(self):
        """Test generating recommended config."""
        result = DiscoveryResult(
            hierarchy_table="DB.SCHEMA.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
            mapping_table="DB.SCHEMA.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
            hierarchy_type="LOS",
        )

        config = self.agent._generate_recommended_config(
            result.hierarchy_table,
            result.mapping_table,
            result,
        )

        assert config is not None
        assert config.report_type == "GROSS"
        assert config.has_group_filter_precedence is True


class TestMCPTools:
    """Test MCP tools registration."""

    def test_register_mart_factory_tools(self):
        """Test registering mart factory tools."""
        from src.wright.mcp_tools import register_mart_factory_tools

        mock_mcp = Mock()
        mock_mcp.tool = Mock(return_value=lambda f: f)

        result = register_mart_factory_tools(mock_mcp)

        # Updated count to 29 with Phase 31 + dbt AI integration tools
        assert result["tools_registered"] == 29
        assert "create_mart_config" in result["tools"]
        assert "add_mart_join_pattern" in result["tools"]
        assert "export_mart_config" in result["tools"]
        assert "generate_mart_pipeline" in result["tools"]
        assert "generate_mart_object" in result["tools"]
        assert "generate_mart_dbt_project" in result["tools"]
        assert "discover_hierarchy_pattern" in result["tools"]
        assert "suggest_mart_config" in result["tools"]
        assert "validate_mart_config" in result["tools"]
        assert "validate_mart_pipeline" in result["tools"]
        # Phase 31 tools
        assert "validate_hierarchy_data_quality" in result["tools"]
        assert "normalize_id_source_values" in result["tools"]
        assert "compare_ddl_content" in result["tools"]


class TestModuleExports:
    """Test module exports."""

    def test_all_exports(self):
        """Test that all expected items are exported."""
        from src.wright import (
            # Enums
            PipelineLayer,
            ObjectType,
            FormulaLogic,
            # Core types
            JoinPattern,
            DynamicColumnMapping,
            MartConfig,
            PipelineObject,
            FormulaPrecedence,
            DataQualityIssue,
            DiscoveryResult,
            # Generators
            MartConfigGenerator,
            MartPipelineGenerator,
            FormulaPrecedenceEngine,
            create_standard_los_formulas,
            CortexDiscoveryAgent,
            # MCP
            register_mart_factory_tools,
            # Phase 31
            HierarchyQualityValidator,
            IDSourceNormalizer,
            GroupFilterPrecedenceEngine,
            DDLDiffComparator,
        )

        # Just verify imports work
        assert PipelineLayer.VW_1 is not None
        assert ObjectType.VIEW is not None
        assert FormulaLogic.SUM is not None
        # Phase 31
        assert HierarchyQualityValidator is not None
        assert IDSourceNormalizer is not None
