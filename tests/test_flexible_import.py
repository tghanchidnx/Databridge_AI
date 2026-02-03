"""Tests for the Flexible Hierarchy Import System."""
import pytest
import json
from src.hierarchy.service import HierarchyService
from src.hierarchy.flexible_import import (
    FlexibleImportService,
    FormatDetector,
    FormatTier,
    InputFormat,
    ProjectDefaults,
)


@pytest.fixture
def service():
    """Create a HierarchyService with test data directory."""
    return HierarchyService("data")


@pytest.fixture
def flex_service(service):
    """Create a FlexibleImportService."""
    return FlexibleImportService(service)


@pytest.fixture
def test_project(service):
    """Create a test project and clean up after."""
    project = service.create_project("Test Project", "For testing")
    yield project
    service.delete_project(project.id)


class TestFormatDetector:
    """Tests for FormatDetector class."""

    def test_detect_csv_format(self):
        """Test CSV format detection."""
        content = "col1,col2\nval1,val2"
        result = FormatDetector.detect_format(content)
        assert result == InputFormat.CSV

    def test_detect_json_format(self):
        """Test JSON format detection."""
        content = '[{"key": "value"}]'
        result = FormatDetector.detect_format(content)
        assert result == InputFormat.JSON

    def test_detect_tier_1(self):
        """Test Tier 1 detection (ultra-simple)."""
        columns = ["source_value", "group_name"]
        result = FormatDetector.detect_tier(columns)
        assert result == FormatTier.TIER_1

    def test_detect_tier_2(self):
        """Test Tier 2 detection (basic with parents)."""
        columns = ["hierarchy_name", "parent_name", "source_value", "sort_order"]
        result = FormatDetector.detect_tier(columns)
        assert result == FormatTier.TIER_2

    def test_detect_tier_3(self):
        """Test Tier 3 detection (standard)."""
        columns = ["hierarchy_id", "hierarchy_name", "parent_id", "source_database", "source_schema"]
        result = FormatDetector.detect_tier(columns)
        assert result == FormatTier.TIER_3

    def test_detect_tier_4(self):
        """Test Tier 4 detection (enterprise)."""
        columns = ["hierarchy_id", "level_1", "level_2", "level_3", "level_1_sort", "include_flag"]
        result = FormatDetector.detect_tier(columns)
        assert result == FormatTier.TIER_4

    def test_analyze_full(self):
        """Test full analysis of content."""
        content = "source_value,group_name\n4100,Revenue\n5100,COGS"
        result = FormatDetector.analyze(content)

        assert result["format"] == "csv"
        assert result["tier"] == "tier_1"
        assert result["columns_found"] == ["source_value", "group_name"]
        assert result["parent_strategy"] == "flat"


class TestProjectDefaults:
    """Tests for ProjectDefaults class."""

    def test_defaults_incomplete(self):
        """Test incomplete defaults."""
        defaults = ProjectDefaults()
        assert not defaults.is_complete()

    def test_defaults_complete(self):
        """Test complete defaults."""
        defaults = ProjectDefaults(
            source_database="DB",
            source_schema="SCHEMA",
            source_table="TABLE",
            source_column="COL"
        )
        assert defaults.is_complete()

    def test_defaults_to_dict(self):
        """Test conversion to dict."""
        defaults = ProjectDefaults(source_database="TEST")
        d = defaults.to_dict()
        assert d["source_database"] == "TEST"
        assert d["source_column"] == ""


class TestFlexibleImportService:
    """Tests for FlexibleImportService class."""

    def test_configure_defaults(self, flex_service, test_project):
        """Test configuring project defaults."""
        defaults = flex_service.configure_defaults(
            project_id=test_project.id,
            source_database="WAREHOUSE",
            source_schema="FINANCE",
            source_table="DIM_ACCOUNT",
            source_column="CODE"
        )

        assert defaults.is_complete()
        assert defaults.source_database == "WAREHOUSE"

        # Verify retrieval
        retrieved = flex_service.get_defaults(test_project.id)
        assert retrieved.source_database == "WAREHOUSE"

    def test_preview_tier_1(self, flex_service):
        """Test preview for Tier 1 import."""
        content = "source_value,group_name\n4100,Revenue\n5100,COGS"
        result = flex_service.preview_import(
            content=content,
            source_defaults={"source_database": "DB", "source_schema": "S", "source_table": "T", "source_column": "C"}
        )

        assert result["detected_tier"] == "tier_1"
        assert result["total_rows"] == 2
        assert result["source_defaults_complete"]

    def test_import_tier_1(self, flex_service, test_project):
        """Test Tier 1 import."""
        content = "source_value,group_name\n4100,Revenue\n4200,Revenue\n5100,COGS"
        result = flex_service.import_flexible(
            project_id=test_project.id,
            content=content,
            source_defaults={
                "source_database": "WAREHOUSE",
                "source_schema": "FINANCE",
                "source_table": "DIM_ACCOUNT",
                "source_column": "CODE"
            }
        )

        assert result["status"] == "success"
        assert result["hierarchies_created"] == 2  # Revenue and COGS
        assert result["mappings_created"] == 3
        assert result["detected_tier"] == "tier_1"

    def test_import_tier_2(self, flex_service, test_project):
        """Test Tier 2 import with parent-child relationships."""
        content = """hierarchy_name,parent_name,source_value,sort_order
Revenue,,4%,1
Product Rev,Revenue,41%,2
Service Rev,Revenue,42%,3"""

        result = flex_service.import_flexible(
            project_id=test_project.id,
            content=content,
            source_defaults={
                "source_database": "DB",
                "source_schema": "S",
                "source_table": "T",
                "source_column": "C"
            }
        )

        assert result["status"] == "success"
        assert result["hierarchies_created"] == 3
        assert result["detected_tier"] == "tier_2"

    def test_export_simplified_tier_1(self, flex_service, service, test_project):
        """Test exporting as Tier 1 format."""
        # Create some hierarchies with mappings
        h1 = service.create_hierarchy(test_project.id, "Revenue")
        service.add_source_mapping(test_project.id, h1.hierarchy_id, "DB", "S", "T", "C", "4100")

        result = flex_service.export_simplified(test_project.id, "tier_1")

        assert result["format"] == "tier_1"
        assert "4100,Revenue" in result["csv_content"]

    def test_export_simplified_tier_2(self, flex_service, service, test_project):
        """Test exporting as Tier 2 format."""
        # Create parent-child hierarchies
        parent = service.create_hierarchy(test_project.id, "Revenue")
        child = service.create_hierarchy(test_project.id, "Product Rev", parent_id=parent.id)

        result = flex_service.export_simplified(test_project.id, "tier_2")

        assert result["format"] == "tier_2"
        assert "hierarchy_name,parent_name" in result["csv_content"]
        assert '"Product Rev","Revenue"' in result["csv_content"]


class TestPropertyImport:
    """Tests for property column detection and import."""

    def test_detect_property_columns(self):
        """Test detection of property columns by prefix."""
        columns = [
            "hierarchy_name",
            "parent_name",
            "PROP_custom_field",
            "DIM_aggregation_type",
            "FACT_measure_type",
            "FILTER_cascading",
            "DISPLAY_visible",
            "template_id",
        ]
        result = FormatDetector.detect_property_columns(columns)

        assert len(result) == 5  # 5 property columns
        assert "PROP_custom_field" in result
        assert result["PROP_custom_field"]["category"] == "custom"
        assert result["PROP_custom_field"]["name"] == "custom_field"
        assert "DIM_aggregation_type" in result
        assert result["DIM_aggregation_type"]["category"] == "dimension"

    def test_import_tier_2_with_properties(self, test_project, service):
        """Test Tier 2 import with property columns."""
        flex_service = FlexibleImportService(service)
        csv_content = """hierarchy_name,parent_name,source_value,DIM_drill_enabled,PROP_cost_center,template_id
Revenue,,,true,CC001,financial_dimension
Product Rev,Revenue,41%,true,CC002,
Service Rev,Revenue,42%,false,CC003,"""

        # Configure defaults
        flex_service.configure_defaults(
            project_id=test_project.id,
            source_database="WAREHOUSE",
            source_schema="FINANCE",
            source_table="DIM_ACCOUNT",
            source_column="ACCOUNT_CODE",
        )

        result = flex_service.import_flexible(
            project_id=test_project.id,
            content=csv_content,
        )

        assert result["status"] == "success"
        assert result["hierarchies_created"] == 3

        # Verify properties were applied
        # Get the hierarchy_id from the result
        revenue_h = next(
            (h for h in result["created_hierarchies"] if h["hierarchy_name"] == "Revenue"),
            None,
        )
        assert revenue_h is not None

        # Check if properties exist
        props = service.get_properties(test_project.id, revenue_h["hierarchy_id"])
        # Should have at least the properties from import
        prop_names = [p["name"] for p in props]
        assert "drill_enabled" in prop_names or "cost_center" in prop_names

    def test_extract_properties_from_row(self, flex_service):
        """Test the property extraction helper method."""
        row = {
            "hierarchy_name": "Test",
            "PROP_custom_value": "test_value",
            "DIM_totals_enabled": "true",
            "FACT_measure_type": "additive",
            "template_id": "financial_dimension",
        }
        columns = list(row.keys())

        properties, template_id = flex_service._extract_properties_from_row(row, columns)

        assert template_id == "financial_dimension"
        assert len(properties) == 3

        # Check custom property
        custom_prop = next((p for p in properties if p["name"] == "custom_value"), None)
        assert custom_prop is not None
        assert custom_prop["category"] == "custom"
        assert custom_prop["value"] == "test_value"

        # Check boolean parsing
        dim_prop = next((p for p in properties if p["name"] == "totals_enabled"), None)
        assert dim_prop is not None
        assert dim_prop["value"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
