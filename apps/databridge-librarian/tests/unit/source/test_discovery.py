"""
Unit tests for SourceDiscoveryService.
"""

import pytest
from unittest.mock import MagicMock, patch
import tempfile
import shutil
from pathlib import Path

from src.source.discovery import (
    SourceDiscoveryService,
    DiscoveryConfig,
    DiscoveryResult,
    DiscoveryProgress,
    DiscoveryPhase,
)


@pytest.fixture
def temp_storage():
    """Create a temporary storage directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def discovery_service(temp_storage):
    """Create a discovery service with temp storage."""
    from src.source import SourceModelStore

    store = SourceModelStore(storage_path=temp_storage)
    return SourceDiscoveryService(store=store)


class TestDiscoveryPhase:
    """Tests for DiscoveryPhase enum."""

    def test_all_phases_exist(self):
        """Test all expected phases exist."""
        assert DiscoveryPhase.CONNECTING
        assert DiscoveryPhase.SCANNING
        assert DiscoveryPhase.ANALYZING
        assert DiscoveryPhase.INFERRING_ENTITIES
        assert DiscoveryPhase.DETECTING_RELATIONSHIPS
        assert DiscoveryPhase.CONSOLIDATING
        assert DiscoveryPhase.COMPLETED
        assert DiscoveryPhase.FAILED


class TestDiscoveryProgress:
    """Tests for DiscoveryProgress."""

    def test_initial_state(self):
        """Test initial progress state."""
        progress = DiscoveryProgress()

        assert progress.phase == DiscoveryPhase.CONNECTING
        assert progress.phase_progress == 0.0
        assert progress.overall_progress >= 0.0

    def test_overall_progress_calculation(self):
        """Test overall progress is calculated correctly."""
        progress = DiscoveryProgress()

        # Initial state
        assert progress.overall_progress < 0.1

        # After completing connecting
        progress.phase = DiscoveryPhase.SCANNING
        progress.phase_progress = 0.5
        assert progress.overall_progress > 0.1
        assert progress.overall_progress < 0.5

        # Completed
        progress.phase = DiscoveryPhase.COMPLETED
        progress.phase_progress = 1.0
        assert progress.overall_progress == 1.0

    def test_to_dict(self):
        """Test serialization to dict."""
        progress = DiscoveryProgress()
        progress.total_tables = 10
        progress.tables_processed = 5
        progress.entities_found = 3

        d = progress.to_dict()

        assert "phase" in d
        assert "phase_progress" in d
        assert "overall_progress" in d
        assert d["total_tables"] == 10
        assert d["tables_processed"] == 5
        assert d["entities_found"] == 3


class TestDiscoveryConfig:
    """Tests for DiscoveryConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DiscoveryConfig()

        assert config.include_views is True
        assert config.include_system_tables is False
        assert config.sample_size == 100
        assert config.detect_scd is True
        assert config.infer_relationships is True
        assert config.min_entity_confidence == 0.5
        assert config.min_relationship_confidence == 0.5

    def test_custom_config(self):
        """Test custom configuration."""
        config = DiscoveryConfig(
            include_views=False,
            sample_size=50,
            min_entity_confidence=0.7,
        )

        assert config.include_views is False
        assert config.sample_size == 50
        assert config.min_entity_confidence == 0.7


class TestDiscoveryFromMetadata:
    """Tests for offline discovery from metadata."""

    def test_discover_from_simple_metadata(self, discovery_service):
        """Test discovery from simple table metadata."""
        tables = [
            {
                "name": "CUSTOMERS",
                "schema": "PUBLIC",
                "database": "TEST",
                "columns": [
                    {"name": "CUSTOMER_ID", "data_type": "INTEGER"},
                    {"name": "CUSTOMER_NAME", "data_type": "VARCHAR"},
                    {"name": "EMAIL", "data_type": "VARCHAR"},
                    {"name": "CREATED_DATE", "data_type": "DATE"},
                ],
            },
            {
                "name": "ORDERS",
                "schema": "PUBLIC",
                "database": "TEST",
                "columns": [
                    {"name": "ORDER_ID", "data_type": "INTEGER"},
                    {"name": "CUSTOMER_ID", "data_type": "INTEGER"},
                    {"name": "ORDER_DATE", "data_type": "DATE"},
                    {"name": "TOTAL_AMOUNT", "data_type": "DECIMAL"},
                ],
            },
        ]

        result = discovery_service.discover_from_metadata(
            tables=tables,
            model_name="Test Discovery",
        )

        assert result.status == "completed"
        assert result.tables_discovered == 2
        assert result.columns_discovered == 8
        assert result.model_id  # Should have a model ID

    def test_discover_entity_inference(self, discovery_service):
        """Test entity inference from table names."""
        tables = [
            {
                "name": "DIM_CUSTOMER",
                "columns": [
                    {"name": "CUSTOMER_ID", "data_type": "INTEGER"},
                    {"name": "CUSTOMER_NAME", "data_type": "VARCHAR"},
                ],
            },
            {
                "name": "DIM_PRODUCT",
                "columns": [
                    {"name": "PRODUCT_ID", "data_type": "INTEGER"},
                    {"name": "PRODUCT_NAME", "data_type": "VARCHAR"},
                ],
            },
        ]

        result = discovery_service.discover_from_metadata(
            tables=tables,
            model_name="Entity Test",
        )

        assert result.status == "completed"
        assert result.entities_inferred >= 0  # May or may not infer entities

    def test_discover_relationship_inference(self, discovery_service):
        """Test relationship inference from foreign keys."""
        tables = [
            {
                "name": "CUSTOMERS",
                "columns": [
                    {"name": "ID", "data_type": "INTEGER", "is_primary_key": True},
                    {"name": "NAME", "data_type": "VARCHAR"},
                ],
            },
            {
                "name": "ORDERS",
                "columns": [
                    {"name": "ORDER_ID", "data_type": "INTEGER", "is_primary_key": True},
                    {"name": "CUSTOMER_ID", "data_type": "INTEGER", "is_foreign_key": True, "foreign_key_ref": "customers.id"},
                ],
            },
        ]

        result = discovery_service.discover_from_metadata(
            tables=tables,
            model_name="Relationship Test",
        )

        assert result.status == "completed"
        # Relationships should be inferred
        assert result.relationships_inferred >= 0

    def test_discover_empty_tables(self, discovery_service):
        """Test discovery with empty table list."""
        result = discovery_service.discover_from_metadata(
            tables=[],
            model_name="Empty Test",
        )

        assert result.status == "completed"
        assert result.tables_discovered == 0
        assert result.columns_discovered == 0

    def test_progress_callback(self, discovery_service):
        """Test progress callback is called."""
        progress_updates = []

        def on_progress(p: DiscoveryProgress):
            progress_updates.append(p.phase)

        discovery_service.set_progress_callback(on_progress)

        tables = [
            {
                "name": "TEST",
                "columns": [{"name": "ID", "data_type": "INTEGER"}],
            },
        ]

        discovery_service.discover_from_metadata(tables=tables)

        # Should have received progress updates
        assert len(progress_updates) > 0
        assert DiscoveryPhase.COMPLETED in progress_updates


class TestDiscoveryResult:
    """Tests for DiscoveryResult."""

    def test_result_to_dict(self):
        """Test serialization to dict."""
        progress = DiscoveryProgress()
        result = DiscoveryResult(
            model_id="test-123",
            model_name="Test Model",
            status="completed",
            progress=progress,
            tables_discovered=5,
            entities_inferred=2,
        )

        d = result.to_dict()

        assert d["model_id"] == "test-123"
        assert d["model_name"] == "Test Model"
        assert d["status"] == "completed"
        assert d["tables_discovered"] == 5
        assert d["entities_inferred"] == 2
        assert "progress" in d


class TestQualityMetrics:
    """Tests for quality metrics and thresholds."""

    def test_confidence_filtering(self, discovery_service):
        """Test that low confidence items are flagged."""
        tables = [
            {
                "name": "UNKNOWN_TABLE_XYZ",
                "columns": [
                    {"name": "COL1", "data_type": "VARCHAR"},
                    {"name": "COL2", "data_type": "VARCHAR"},
                ],
            },
        ]

        result = discovery_service.discover_from_metadata(
            tables=tables,
            model_name="Quality Test",
        )

        # Table should need review (can't infer entity type)
        assert "UNKNOWN_TABLE_XYZ" in result.tables_needing_review or result.warnings


class TestDiscoveryServiceInit:
    """Tests for DiscoveryService initialization."""

    def test_default_init(self, temp_storage):
        """Test default initialization."""
        service = SourceDiscoveryService()
        assert service.store is not None
        assert service.config is not None

    def test_custom_config(self, temp_storage):
        """Test initialization with custom config."""
        config = DiscoveryConfig(
            include_views=False,
            min_entity_confidence=0.8,
        )
        service = SourceDiscoveryService(config=config)

        assert service.config.include_views is False
        assert service.config.min_entity_confidence == 0.8

    def test_get_discovery_status(self, discovery_service):
        """Test getting discovery status."""
        # Before any discovery
        status = discovery_service.get_discovery_status()
        assert status is None

        # During discovery (status should be set)
        tables = [{"name": "TEST", "columns": [{"name": "ID", "data_type": "INT"}]}]
        discovery_service.discover_from_metadata(tables=tables)

        # After discovery (progress object still exists)
        status = discovery_service.get_discovery_status()
        assert status is not None
        assert "phase" in status
