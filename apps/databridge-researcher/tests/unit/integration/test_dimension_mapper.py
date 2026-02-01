"""
Tests for Dimension Mapper.

Tests the mapping of V3 hierarchies to V4 dimension structures.
"""

import pytest
from unittest.mock import Mock, patch

from src.integration.dimension_mapper import (
    DimensionMapper,
    DimensionType,
    Dimension,
    DimensionAttribute,
    DimensionMember,
    DimensionMapperResult,
)
from src.integration.v3_client import V3Hierarchy, V3Mapping


class TestDimensionType:
    """Tests for DimensionType enum."""

    def test_dimension_types_exist(self):
        """Test all expected dimension types exist."""
        assert DimensionType.ACCOUNT.value == "account"
        assert DimensionType.ENTITY.value == "entity"
        assert DimensionType.GEOGRAPHY.value == "geography"
        assert DimensionType.TIME.value == "time"
        assert DimensionType.PRODUCT.value == "product"
        assert DimensionType.CUSTOMER.value == "customer"
        assert DimensionType.PROJECT.value == "project"
        assert DimensionType.CUSTOM.value == "custom"


class TestDimensionAttribute:
    """Tests for DimensionAttribute dataclass."""

    def test_attribute_creation(self):
        """Test creating a dimension attribute."""
        attr = DimensionAttribute(
            name="Level 1",
            data_type="string",
            source_level="level_1",
            is_key=True,
        )

        assert attr.name == "Level 1"
        assert attr.data_type == "string"
        assert attr.source_level == "level_1"
        assert attr.is_key is True

    def test_attribute_to_dict(self):
        """Test attribute to dictionary conversion."""
        attr = DimensionAttribute(
            name="Account Code",
            data_type="string",
            is_key=True,
            is_display=False,
            sort_order=1,
        )

        result = attr.to_dict()

        assert result["name"] == "Account Code"
        assert result["is_key"] is True
        assert result["sort_order"] == 1

    def test_attribute_defaults(self):
        """Test attribute default values."""
        attr = DimensionAttribute(name="Test")

        assert attr.data_type == "string"
        assert attr.source_level is None
        assert attr.is_key is False
        assert attr.is_display is False
        assert attr.sort_order == 0


class TestDimensionMember:
    """Tests for DimensionMember dataclass."""

    def test_member_creation(self):
        """Test creating a dimension member."""
        member = DimensionMember(
            key="ACC-001",
            name="Revenue",
            parent_key=None,
            level=0,
        )

        assert member.key == "ACC-001"
        assert member.name == "Revenue"
        assert member.parent_key is None
        assert member.level == 0

    def test_member_with_parent(self):
        """Test creating a member with parent."""
        member = DimensionMember(
            key="ACC-002",
            name="Sales Revenue",
            parent_key="ACC-001",
            level=1,
            is_leaf=True,
        )

        assert member.parent_key == "ACC-001"
        assert member.level == 1
        assert member.is_leaf is True

    def test_member_to_dict(self):
        """Test member to dictionary conversion."""
        member = DimensionMember(
            key="M1",
            name="Member 1",
            parent_key="P1",
            level=2,
            attributes={"region": "North"},
            sort_order=5,
            is_leaf=False,
        )

        result = member.to_dict()

        assert result["key"] == "M1"
        assert result["parent_key"] == "P1"
        assert result["attributes"]["region"] == "North"
        assert result["is_leaf"] is False


class TestDimension:
    """Tests for Dimension dataclass."""

    def test_dimension_creation(self):
        """Test creating a dimension."""
        dim = Dimension(
            name="Account",
            dimension_type=DimensionType.ACCOUNT,
            description="Chart of accounts",
        )

        assert dim.name == "Account"
        assert dim.dimension_type == DimensionType.ACCOUNT
        assert dim.description == "Chart of accounts"

    def test_dimension_with_members(self):
        """Test dimension with members."""
        members = [
            DimensionMember(key="1", name="Root", level=0, is_leaf=False),
            DimensionMember(key="2", name="Child 1", parent_key="1", level=1, is_leaf=True),
            DimensionMember(key="3", name="Child 2", parent_key="1", level=1, is_leaf=True),
        ]

        dim = Dimension(
            name="Test",
            dimension_type=DimensionType.CUSTOM,
            members=members,
        )

        assert len(dim.members) == 3

    def test_dimension_to_dict(self):
        """Test dimension to dictionary conversion."""
        dim = Dimension(
            name="Geography",
            dimension_type=DimensionType.GEOGRAPHY,
            hierarchy_id="h-123",
            project_id="p-123",
        )

        result = dim.to_dict()

        assert result["name"] == "Geography"
        assert result["dimension_type"] == "geography"
        assert result["hierarchy_id"] == "h-123"
        assert result["member_count"] == 0

    def test_get_leaf_members(self):
        """Test getting leaf members."""
        members = [
            DimensionMember(key="1", name="Root", level=0, is_leaf=False),
            DimensionMember(key="2", name="Leaf 1", level=1, is_leaf=True),
            DimensionMember(key="3", name="Leaf 2", level=1, is_leaf=True),
        ]

        dim = Dimension(
            name="Test",
            dimension_type=DimensionType.CUSTOM,
            members=members,
        )

        leaves = dim.get_leaf_members()

        assert len(leaves) == 2
        assert all(m.is_leaf for m in leaves)

    def test_get_members_at_level(self):
        """Test getting members at a specific level."""
        members = [
            DimensionMember(key="1", name="Level 0", level=0),
            DimensionMember(key="2", name="Level 1 A", level=1),
            DimensionMember(key="3", name="Level 1 B", level=1),
            DimensionMember(key="4", name="Level 2", level=2),
        ]

        dim = Dimension(
            name="Test",
            dimension_type=DimensionType.CUSTOM,
            members=members,
        )

        level_1 = dim.get_members_at_level(1)

        assert len(level_1) == 2
        assert all(m.level == 1 for m in level_1)


class TestDimensionMapperResult:
    """Tests for DimensionMapperResult dataclass."""

    def test_result_success(self):
        """Test successful result."""
        dim = Dimension(name="Test", dimension_type=DimensionType.CUSTOM)
        result = DimensionMapperResult(
            success=True,
            message="Mapping complete",
            dimension=dim,
        )

        assert result.success is True
        assert result.dimension.name == "Test"

    def test_result_failure(self):
        """Test failed result."""
        result = DimensionMapperResult(
            success=False,
            message="Mapping failed",
            errors=["Invalid hierarchy"],
        )

        assert result.success is False
        assert len(result.errors) == 1

    def test_result_to_dict(self):
        """Test result to dictionary conversion."""
        dim = Dimension(name="Test", dimension_type=DimensionType.ACCOUNT)
        result = DimensionMapperResult(
            success=True,
            dimension=dim,
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["dimension"]["name"] == "Test"


class TestDimensionMapper:
    """Tests for DimensionMapper."""

    def test_mapper_initialization(self):
        """Test mapper initialization."""
        mapper = DimensionMapper(
            default_dimension_type=DimensionType.CUSTOM,
            level_name_format="L{n}",
        )

        assert mapper.default_dimension_type == DimensionType.CUSTOM
        assert mapper.level_name_format == "L{n}"

    def test_mapper_defaults(self):
        """Test mapper default values."""
        mapper = DimensionMapper()

        assert mapper.default_dimension_type == DimensionType.CUSTOM
        assert mapper.infer_types is True

    def test_infer_dimension_type_account(self):
        """Test inferring account dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Chart of Accounts")
        assert result == DimensionType.ACCOUNT

        result = mapper._infer_dimension_type("GL Account Hierarchy")
        assert result == DimensionType.ACCOUNT

        result = mapper._infer_dimension_type("Revenue and Expense")
        assert result == DimensionType.ACCOUNT

    def test_infer_dimension_type_entity(self):
        """Test inferring entity dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Legal Entity")
        assert result == DimensionType.ENTITY

        result = mapper._infer_dimension_type("Company Structure")
        assert result == DimensionType.ENTITY

    def test_infer_dimension_type_geography(self):
        """Test inferring geography dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Geographic Regions")
        assert result == DimensionType.GEOGRAPHY

        result = mapper._infer_dimension_type("Country Hierarchy")
        assert result == DimensionType.GEOGRAPHY

    def test_infer_dimension_type_time(self):
        """Test inferring time dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Time Period")
        assert result == DimensionType.TIME

        result = mapper._infer_dimension_type("Fiscal Year")
        assert result == DimensionType.TIME

    def test_infer_dimension_type_product(self):
        """Test inferring product dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Product Categories")
        assert result == DimensionType.PRODUCT

        result = mapper._infer_dimension_type("SKU Hierarchy")
        assert result == DimensionType.PRODUCT

    def test_infer_dimension_type_customer(self):
        """Test inferring customer dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Customer Segments")
        assert result == DimensionType.CUSTOMER

    def test_infer_dimension_type_project(self):
        """Test inferring project dimension type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Project Hierarchy")
        assert result == DimensionType.PROJECT

        result = mapper._infer_dimension_type("Work Order Tracking")
        assert result == DimensionType.PROJECT

        result = mapper._infer_dimension_type("Department Structure")
        assert result == DimensionType.PROJECT

        # "job" keyword matches PROJECT
        result = mapper._infer_dimension_type("Job Structure")
        assert result == DimensionType.PROJECT

    def test_infer_dimension_type_custom(self):
        """Test falling back to custom type."""
        mapper = DimensionMapper()

        result = mapper._infer_dimension_type("Unknown Dimension")
        assert result == DimensionType.CUSTOM

    def test_map_hierarchy_basic(self):
        """Test mapping a basic hierarchy."""
        hierarchy = V3Hierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="GL Accounts",
            levels={"level_1": "Total", "level_2": "Revenue"},
        )

        mapper = DimensionMapper()
        result = mapper.map_hierarchy(hierarchy)

        assert result.success is True
        assert result.dimension.name == "GL Accounts"
        assert result.dimension.dimension_type == DimensionType.ACCOUNT

    def test_map_hierarchy_with_explicit_type(self):
        """Test mapping with explicit dimension type."""
        hierarchy = V3Hierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Custom Hierarchy",
            levels={"level_1": "Root"},
        )

        mapper = DimensionMapper()
        result = mapper.map_hierarchy(hierarchy, dimension_type=DimensionType.ENTITY)

        assert result.success is True
        assert result.dimension.dimension_type == DimensionType.ENTITY

    def test_map_hierarchy_with_mappings(self):
        """Test mapping hierarchy with source mappings."""
        hierarchy = V3Hierarchy(
            hierarchy_id="h-123",
            project_id="p-123",
            hierarchy_name="Accounts",
            levels={"level_1": "Total"},
        )

        mappings = [
            V3Mapping(
                hierarchy_id="h-123",
                mapping_index=0,
                source_database="prod",
                source_schema="finance",
                source_table="gl",
                source_column="account",
            ),
        ]

        mapper = DimensionMapper()
        result = mapper.map_hierarchy(hierarchy, mappings=mappings)

        assert result.success is True
        assert len(result.dimension.source_mappings) == 1

    def test_map_hierarchy_tree(self):
        """Test mapping a hierarchy tree."""
        hierarchies = [
            V3Hierarchy(
                hierarchy_id="h-1",
                project_id="p-1",
                hierarchy_name="Root",
                parent_id=None,
                levels={"level_1": "Total"},
            ),
            V3Hierarchy(
                hierarchy_id="h-2",
                project_id="p-1",
                hierarchy_name="Child 1",
                parent_id="h-1",
                levels={"level_1": "Total", "level_2": "C1"},
            ),
            V3Hierarchy(
                hierarchy_id="h-3",
                project_id="p-1",
                hierarchy_name="Child 2",
                parent_id="h-1",
                levels={"level_1": "Total", "level_2": "C2"},
            ),
        ]

        mapper = DimensionMapper()
        result = mapper.map_hierarchy_tree(hierarchies, "Account Dimension")

        assert result.success is True
        assert result.dimension.name == "Account Dimension"
        assert len(result.dimension.members) == 3

    def test_map_hierarchy_tree_leaf_detection(self):
        """Test that leaf members are correctly identified."""
        hierarchies = [
            V3Hierarchy(
                hierarchy_id="h-1",
                project_id="p-1",
                hierarchy_name="Root",
                parent_id=None,
                levels={},
            ),
            V3Hierarchy(
                hierarchy_id="h-2",
                project_id="p-1",
                hierarchy_name="Leaf",
                parent_id="h-1",
                levels={},
            ),
        ]

        mapper = DimensionMapper()
        result = mapper.map_hierarchy_tree(hierarchies, "Test")

        # Root should not be leaf, child should be
        members = result.dimension.members
        root_member = next(m for m in members if m.key == "h-1")
        leaf_member = next(m for m in members if m.key == "h-2")

        assert root_member.is_leaf is False
        assert leaf_member.is_leaf is True

    def test_map_hierarchy_tree_empty(self):
        """Test mapping empty hierarchy tree."""
        mapper = DimensionMapper()
        result = mapper.map_hierarchy_tree([], "Empty")

        assert result.success is False
        assert "No hierarchies provided" in result.errors[0]

    def test_map_hierarchies_to_dimensions(self):
        """Test mapping multiple hierarchies to dimensions."""
        hierarchies = [
            V3Hierarchy(
                hierarchy_id="h-1",
                project_id="p-1",
                hierarchy_name="Accounts",
                levels={},
            ),
            V3Hierarchy(
                hierarchy_id="h-2",
                project_id="p-2",
                hierarchy_name="Regions",
                levels={},
            ),
        ]

        mapper = DimensionMapper()
        result = mapper.map_hierarchies_to_dimensions(hierarchies, group_by="project_id")

        assert result.success is True
        assert len(result.dimensions) == 2

    def test_generate_dimension_sql(self):
        """Test generating SQL for a dimension."""
        dim = Dimension(
            name="Account",
            dimension_type=DimensionType.ACCOUNT,
            attributes=[
                DimensionAttribute(name="Level 1", source_level="level_1"),
                DimensionAttribute(name="Level 2", source_level="level_2"),
            ],
        )

        mapper = DimensionMapper()
        sql = mapper.generate_dimension_sql(dim)

        assert "CREATE TABLE dim_account" in sql
        assert "account_key VARCHAR(100) PRIMARY KEY" in sql
        assert "level_1 VARCHAR(255)" in sql
        assert "level_2 VARCHAR(255)" in sql

    def test_generate_dimension_sql_custom_table(self):
        """Test generating SQL with custom table name."""
        dim = Dimension(
            name="Test Dimension",
            dimension_type=DimensionType.CUSTOM,
        )

        mapper = DimensionMapper()
        sql = mapper.generate_dimension_sql(dim, table_name="custom_dim_table")

        assert "CREATE TABLE custom_dim_table" in sql

    def test_create_level_attributes(self):
        """Test creating attributes from hierarchy levels."""
        hierarchy = V3Hierarchy(
            hierarchy_id="h-1",
            project_id="p-1",
            hierarchy_name="Test",
            levels={
                "level_1": "A",
                "level_2": "B",
                "level_3": "C",
                "level_5": "E",  # Gap in levels
            },
        )

        mapper = DimensionMapper(level_name_format="Lvl {n}")
        attributes = mapper._create_level_attributes(hierarchy)

        # Should create attributes for levels with values
        assert len(attributes) == 4
        assert attributes[0].name == "Lvl 1"
        assert attributes[0].source_level == "level_1"
