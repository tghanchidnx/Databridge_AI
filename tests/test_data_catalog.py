"""
Unit tests for Data Catalog (Phase 29).

Tests cover:
- Data asset CRUD operations
- Business glossary management
- Tag management
- Search functionality
- PII detection
- Catalog statistics
"""

import json
import pytest
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_data_dir():
    """Create a temporary data directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def catalog_store(temp_data_dir):
    """Create a CatalogStore with temp directory."""
    from src.data_catalog.catalog_store import CatalogStore
    return CatalogStore(data_dir=temp_data_dir)


@pytest.fixture
def sample_asset():
    """Create a sample data asset."""
    from src.data_catalog.types import (
        DataAsset,
        AssetType,
        DataClassification,
        DataQualityTier,
        Tag,
        Owner,
        OwnershipRole,
        ColumnProfile,
    )

    return DataAsset(
        name="CUSTOMER_DIM",
        asset_type=AssetType.TABLE,
        description="Customer dimension table",
        database="ANALYTICS",
        schema_name="PUBLIC",
        fully_qualified_name="ANALYTICS.PUBLIC.CUSTOMER_DIM",
        classification=DataClassification.PII,
        quality_tier=DataQualityTier.GOLD,
        tags=[Tag(name="dimension"), Tag(name="customer")],
        owners=[Owner(
            user_id="john_doe",
            name="John Doe",
            email="john@example.com",
            role=OwnershipRole.OWNER,
        )],
        columns=[
            ColumnProfile(
                column_name="CUSTOMER_ID",
                data_type="NUMBER",
                nullable=False,
                is_primary_key=True,
            ),
            ColumnProfile(
                column_name="CUSTOMER_NAME",
                data_type="VARCHAR",
                nullable=True,
                description="Customer full name",
            ),
            ColumnProfile(
                column_name="EMAIL",
                data_type="VARCHAR",
                nullable=True,
                classification=DataClassification.PII,
            ),
        ],
        row_count=100000,
    )


# =============================================================================
# Types Tests
# =============================================================================

class TestDataCatalogTypes:
    """Test Pydantic models for Data Catalog."""

    def test_asset_type_enum(self):
        """Test AssetType enum values."""
        from src.data_catalog.types import AssetType

        assert AssetType.TABLE.value == "table"
        assert AssetType.VIEW.value == "view"
        assert AssetType.HIERARCHY.value == "hierarchy"

    def test_data_classification_enum(self):
        """Test DataClassification enum values."""
        from src.data_catalog.types import DataClassification

        assert DataClassification.PII.value == "pii"
        assert DataClassification.PHI.value == "phi"
        assert DataClassification.CONFIDENTIAL.value == "confidential"

    def test_tag_creation(self):
        """Test Tag model."""
        from src.data_catalog.types import Tag

        tag = Tag(name="validated", category="status", color="#00ff00")
        assert tag.name == "validated"
        assert tag.category == "status"

    def test_owner_creation(self):
        """Test Owner model."""
        from src.data_catalog.types import Owner, OwnershipRole

        owner = Owner(
            user_id="jane_doe",
            name="Jane Doe",
            email="jane@example.com",
            role=OwnershipRole.STEWARD,
        )
        assert owner.user_id == "jane_doe"
        assert owner.role == OwnershipRole.STEWARD

    def test_quality_metrics(self):
        """Test QualityMetrics model."""
        from src.data_catalog.types import QualityMetrics

        metrics = QualityMetrics(
            completeness=95.5,
            uniqueness=100.0,
            accuracy=98.0,
        )
        assert metrics.overall_score == pytest.approx(97.83, rel=0.01)

    def test_data_asset_add_tag(self, sample_asset):
        """Test adding a tag to an asset."""
        from src.data_catalog.types import Tag

        initial_count = len(sample_asset.tags)
        sample_asset.add_tag(Tag(name="new_tag"))
        assert len(sample_asset.tags) == initial_count + 1

        # Adding duplicate tag should not increase count
        sample_asset.add_tag(Tag(name="new_tag"))
        assert len(sample_asset.tags) == initial_count + 1

    def test_data_asset_remove_tag(self, sample_asset):
        """Test removing a tag from an asset."""
        initial_count = len(sample_asset.tags)
        assert sample_asset.remove_tag("dimension") is True
        assert len(sample_asset.tags) == initial_count - 1

        # Removing non-existent tag returns False
        assert sample_asset.remove_tag("nonexistent") is False

    def test_glossary_term_creation(self):
        """Test GlossaryTerm model."""
        from src.data_catalog.types import GlossaryTerm, TermStatus

        term = GlossaryTerm(
            name="Revenue",
            definition="Total income from sales",
            domain="Finance",
            status=TermStatus.APPROVED,
        )
        assert term.name == "Revenue"
        assert term.status == TermStatus.APPROVED


# =============================================================================
# Catalog Store Tests
# =============================================================================

class TestCatalogStore:
    """Test CatalogStore operations."""

    def test_create_asset(self, catalog_store, sample_asset):
        """Test creating a data asset."""
        created = catalog_store.create_asset(sample_asset)
        assert created.id == sample_asset.id
        assert created.name == "CUSTOMER_DIM"

    def test_create_duplicate_asset_fails(self, catalog_store, sample_asset):
        """Test that creating duplicate asset raises error."""
        catalog_store.create_asset(sample_asset)

        with pytest.raises(ValueError, match="already exists"):
            catalog_store.create_asset(sample_asset)

    def test_get_asset_by_id(self, catalog_store, sample_asset):
        """Test getting asset by ID."""
        catalog_store.create_asset(sample_asset)

        retrieved = catalog_store.get_asset(sample_asset.id)
        assert retrieved is not None
        assert retrieved.name == sample_asset.name

    def test_get_asset_by_name(self, catalog_store, sample_asset):
        """Test getting asset by name."""
        catalog_store.create_asset(sample_asset)

        retrieved = catalog_store.get_asset_by_name(
            "CUSTOMER_DIM",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )
        assert retrieved is not None
        assert retrieved.id == sample_asset.id

    def test_update_asset(self, catalog_store, sample_asset):
        """Test updating an asset."""
        catalog_store.create_asset(sample_asset)

        updated = catalog_store.update_asset(sample_asset.id, {
            "description": "Updated description",
            "row_count": 200000,
        })

        assert updated is not None
        assert updated.description == "Updated description"
        assert updated.row_count == 200000

    def test_delete_asset(self, catalog_store, sample_asset):
        """Test deleting an asset."""
        catalog_store.create_asset(sample_asset)

        assert catalog_store.delete_asset(sample_asset.id) is True
        assert catalog_store.get_asset(sample_asset.id) is None

        # Deleting non-existent returns False
        assert catalog_store.delete_asset("nonexistent") is False

    def test_list_assets(self, catalog_store):
        """Test listing assets with filters."""
        from src.data_catalog.types import DataAsset, AssetType

        # Create multiple assets
        for i in range(5):
            asset = DataAsset(
                name=f"TABLE_{i}",
                asset_type=AssetType.TABLE,
                database="ANALYTICS",
            )
            catalog_store.create_asset(asset)

        view = DataAsset(
            name="SALES_VIEW",
            asset_type=AssetType.VIEW,
            database="ANALYTICS",
        )
        catalog_store.create_asset(view)

        # List all
        all_assets = catalog_store.list_assets()
        assert len(all_assets) == 6

        # Filter by type
        tables = catalog_store.list_assets(asset_type=AssetType.TABLE)
        assert len(tables) == 5

        # Filter by database
        analytics = catalog_store.list_assets(database="ANALYTICS")
        assert len(analytics) == 6

    def test_tag_operations(self, catalog_store, sample_asset):
        """Test tag operations."""
        from src.data_catalog.types import Tag

        catalog_store.create_asset(sample_asset)

        # Create tag
        tag = catalog_store.create_tag(Tag(name="validated", category="status"))
        assert tag.name == "validated"

        # List tags
        tags = catalog_store.list_tags()
        assert len(tags) >= 1

        # Add tag to asset
        assert catalog_store.add_tag_to_asset(sample_asset.id, "production") is True

        # Remove tag from asset
        assert catalog_store.remove_tag_from_asset(sample_asset.id, "production") is True

    def test_glossary_term_operations(self, catalog_store):
        """Test glossary term CRUD."""
        from src.data_catalog.types import GlossaryTerm, TermStatus

        # Create term
        term = GlossaryTerm(
            name="Revenue",
            definition="Total income from sales",
            domain="Finance",
        )
        created = catalog_store.create_term(term)
        assert created.id == term.id

        # Get term
        retrieved = catalog_store.get_term(term.id)
        assert retrieved is not None
        assert retrieved.name == "Revenue"

        # Get term by name
        by_name = catalog_store.get_term_by_name("Revenue")
        assert by_name is not None

        # Update term
        updated = catalog_store.update_term(term.id, {
            "status": TermStatus.APPROVED,
        })
        assert updated.status == TermStatus.APPROVED

        # List terms
        terms = catalog_store.list_terms(domain="Finance")
        assert len(terms) >= 1

        # Delete term
        assert catalog_store.delete_term(term.id) is True

    def test_persistence(self, temp_data_dir):
        """Test that data persists across store instances."""
        from src.data_catalog.catalog_store import CatalogStore
        from src.data_catalog.types import DataAsset, AssetType

        # Create and save
        store1 = CatalogStore(data_dir=temp_data_dir)
        asset = DataAsset(
            name="PERSIST_TEST",
            asset_type=AssetType.TABLE,
        )
        store1.create_asset(asset)

        # Load in new instance
        store2 = CatalogStore(data_dir=temp_data_dir)
        retrieved = store2.get_asset(asset.id)

        assert retrieved is not None
        assert retrieved.name == "PERSIST_TEST"


# =============================================================================
# Search Tests
# =============================================================================

class TestCatalogSearch:
    """Test search functionality."""

    def test_basic_search(self, catalog_store):
        """Test basic search by name."""
        from src.data_catalog.types import DataAsset, AssetType, SearchQuery

        # Create test assets
        catalog_store.create_asset(DataAsset(
            name="CUSTOMER_DIM",
            asset_type=AssetType.TABLE,
            description="Customer dimension table",
        ))
        catalog_store.create_asset(DataAsset(
            name="PRODUCT_DIM",
            asset_type=AssetType.TABLE,
            description="Product dimension table",
        ))
        catalog_store.create_asset(DataAsset(
            name="SALES_FACT",
            asset_type=AssetType.TABLE,
            description="Sales fact table for revenue analysis",
        ))

        # Search for "customer"
        query = SearchQuery(query="customer")
        results = catalog_store.search(query)

        assert results.total_count >= 1
        assert any("CUSTOMER" in r.name for r in results.results)

    def test_search_with_filters(self, catalog_store):
        """Test search with asset type filter."""
        from src.data_catalog.types import DataAsset, AssetType, SearchQuery, Tag

        catalog_store.create_asset(DataAsset(
            name="SALES_TABLE",
            asset_type=AssetType.TABLE,
            tags=[Tag(name="sales")],
        ))
        catalog_store.create_asset(DataAsset(
            name="SALES_VIEW",
            asset_type=AssetType.VIEW,
            tags=[Tag(name="sales")],
        ))

        # Search tables only
        query = SearchQuery(
            query="sales",
            asset_types=[AssetType.TABLE],
        )
        results = catalog_store.search(query)

        assert all(r.asset_type == AssetType.TABLE for r in results.results)

    def test_search_in_columns(self, catalog_store, sample_asset):
        """Test search finds matches in column names."""
        from src.data_catalog.types import SearchQuery

        catalog_store.create_asset(sample_asset)

        # Search for column name
        query = SearchQuery(query="email")
        results = catalog_store.search(query)

        assert results.total_count >= 1


# =============================================================================
# Scanner Tests
# =============================================================================

class TestCatalogScanner:
    """Test CatalogScanner functionality."""

    def test_pii_detection(self, catalog_store):
        """Test PII detection in column names."""
        from src.data_catalog.scanner import CatalogScanner
        from src.data_catalog.types import ColumnProfile, DataClassification

        scanner = CatalogScanner(catalog_store)

        columns = [
            ColumnProfile(column_name="ID", data_type="NUMBER"),
            ColumnProfile(column_name="EMAIL_ADDRESS", data_type="VARCHAR"),
            ColumnProfile(column_name="PHONE_NUMBER", data_type="VARCHAR"),
            ColumnProfile(column_name="SSN", data_type="VARCHAR"),
            ColumnProfile(column_name="CREDIT_CARD_NUMBER", data_type="VARCHAR"),
            ColumnProfile(column_name="AMOUNT", data_type="NUMBER"),
        ]

        pii_count = scanner._detect_pii(columns)

        assert pii_count >= 4  # email, phone, ssn, credit_card

        # Check classifications
        email_col = next(c for c in columns if "EMAIL" in c.column_name)
        assert email_col.classification == DataClassification.PII

        cc_col = next(c for c in columns if "CREDIT_CARD" in c.column_name)
        assert cc_col.classification == DataClassification.PCI


# =============================================================================
# Statistics Tests
# =============================================================================

class TestCatalogStats:
    """Test catalog statistics."""

    def test_get_stats(self, catalog_store, sample_asset):
        """Test getting catalog statistics."""
        from src.data_catalog.types import (
            DataAsset,
            AssetType,
            GlossaryTerm,
        )

        # Add assets
        catalog_store.create_asset(sample_asset)
        catalog_store.create_asset(DataAsset(
            name="VIEW_1",
            asset_type=AssetType.VIEW,
        ))

        # Add term
        catalog_store.create_term(GlossaryTerm(
            name="Test Term",
            definition="Test definition",
            domain="Test",
        ))

        stats = catalog_store.get_stats()

        assert stats.total_assets == 2
        assert stats.assets_by_type.get("table", 0) >= 1
        assert stats.total_glossary_terms == 1


# =============================================================================
# MCP Tools Tests
# =============================================================================

class TestDataCatalogMCPTools:
    """Test MCP tool functions."""

    def test_tools_registration(self, temp_data_dir):
        """Test that tools register successfully."""
        from unittest.mock import MagicMock

        mock_mcp = MagicMock()
        mock_mcp.tool = MagicMock(return_value=lambda f: f)

        mock_settings = MagicMock()
        mock_settings.data_dir = temp_data_dir

        from src.data_catalog.mcp_tools import register_data_catalog_tools

        result = register_data_catalog_tools(mock_mcp, mock_settings)

        assert result["tools_registered"] == 15
        assert "asset_management" in result["categories"]
        assert "discovery_scanning" in result["categories"]
        assert "glossary" in result["categories"]
        assert "search_discovery" in result["categories"]


# =============================================================================
# Integration Tests
# =============================================================================

class TestDataCatalogIntegration:
    """Integration tests for Data Catalog."""

    def test_full_workflow(self, temp_data_dir):
        """Test complete catalog workflow."""
        from src.data_catalog.catalog_store import CatalogStore
        from src.data_catalog.types import (
            DataAsset,
            AssetType,
            GlossaryTerm,
            Tag,
            Owner,
            OwnershipRole,
            SearchQuery,
        )

        store = CatalogStore(data_dir=temp_data_dir)

        # 1. Create assets
        customer = DataAsset(
            name="CUSTOMER",
            asset_type=AssetType.TABLE,
            database="ANALYTICS",
            schema_name="PUBLIC",
            description="Customer master data",
        )
        store.create_asset(customer)

        orders = DataAsset(
            name="ORDERS",
            asset_type=AssetType.TABLE,
            database="ANALYTICS",
            schema_name="PUBLIC",
            description="Customer orders",
            upstream_assets=[customer.id],
        )
        store.create_asset(orders)

        # 2. Add tags
        store.add_tag_to_asset(customer.id, "master_data")
        store.add_tag_to_asset(customer.id, "pii")

        # 3. Create glossary term
        term = GlossaryTerm(
            name="Customer",
            definition="An individual or business that purchases goods or services",
            domain="Sales",
        )
        store.create_term(term)

        # 4. Link term to asset
        store.link_term_to_asset(term.id, customer.id)

        # 5. Search
        results = store.search(SearchQuery(query="customer"))
        assert results.total_count >= 1

        # 6. Get stats
        stats = store.get_stats()
        assert stats.total_assets == 2
        assert stats.total_glossary_terms == 1

        # 7. Verify lineage
        lineage = store.get_asset_lineage(orders.id)
        assert len(lineage["upstream"]) == 1
        assert lineage["upstream"][0].name == "CUSTOMER"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
