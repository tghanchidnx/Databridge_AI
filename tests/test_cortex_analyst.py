"""
Unit tests for Cortex Analyst integration (Phase 20).

Tests cover:
- Semantic model types and validation
- SemanticModelManager CRUD operations
- YAML generation
- AnalystClient request/response parsing
- Integration with hierarchy service
"""

import json
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile
import shutil


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
def model_manager(temp_data_dir):
    """Create a SemanticModelManager with temp directory."""
    from src.cortex_agent.semantic_model import SemanticModelManager
    return SemanticModelManager(data_dir=temp_data_dir)


@pytest.fixture
def sample_model_config():
    """Create a sample semantic model configuration."""
    from src.cortex_agent.analyst_types import (
        SemanticModelConfig,
        LogicalTable,
        BaseTableRef,
        Dimension,
        TimeDimension,
        Metric,
    )

    return SemanticModelConfig(
        name="test_sales",
        description="Test sales analytics model",
        database="ANALYTICS",
        schema_name="PUBLIC",
        tables=[
            LogicalTable(
                name="sales",
                description="Sales transactions",
                base_table=BaseTableRef(
                    database="ANALYTICS",
                    schema="PUBLIC",
                    table="SALES_FACT",
                ),
                dimensions=[
                    Dimension(
                        name="region",
                        synonyms=["area", "territory"],
                        description="Sales region",
                        expr="region_name",
                        data_type="VARCHAR",
                    ),
                ],
                time_dimensions=[
                    TimeDimension(
                        name="sale_date",
                        synonyms=["date"],
                        description="Date of sale",
                        expr="sale_date",
                        data_type="DATE",
                    ),
                ],
                metrics=[
                    Metric(
                        name="total_revenue",
                        synonyms=["revenue", "sales"],
                        description="Total revenue",
                        expr="SUM(amount)",
                        data_type="NUMBER",
                    ),
                ],
            ),
        ],
    )


# =============================================================================
# Analyst Types Tests
# =============================================================================

class TestAnalystTypes:
    """Test Pydantic models for Cortex Analyst."""

    def test_base_table_ref(self):
        """Test BaseTableRef creation and fully_qualified method."""
        from src.cortex_agent.analyst_types import BaseTableRef

        ref = BaseTableRef(
            database="DB",
            schema="SCHEMA",
            table="TABLE",
        )
        assert ref.fully_qualified() == "DB.SCHEMA.TABLE"

    def test_dimension(self):
        """Test Dimension model."""
        from src.cortex_agent.analyst_types import Dimension

        dim = Dimension(
            name="region",
            synonyms=["area"],
            description="Sales region",
            expr="REGION_NAME",
            data_type="VARCHAR",
        )
        assert dim.name == "region"
        assert dim.synonyms == ["area"]
        assert dim.unique is False

    def test_metric(self):
        """Test Metric model."""
        from src.cortex_agent.analyst_types import Metric

        metric = Metric(
            name="revenue",
            description="Total revenue",
            expr="SUM(amount)",
            data_type="NUMBER",
        )
        assert metric.name == "revenue"
        assert metric.default_aggregation == "SUM"

    def test_analyst_message_user(self):
        """Test creating a user message."""
        from src.cortex_agent.analyst_types import AnalystMessage

        msg = AnalystMessage.user_message("What is total revenue?")
        assert msg.role == "user"
        assert len(msg.content) == 1
        assert msg.content[0].text == "What is total revenue?"

    def test_analyst_conversation(self):
        """Test conversation tracking."""
        from src.cortex_agent.analyst_types import (
            AnalystConversation,
            AnalystResponse,
            AnalystMessage,
            MessageContent,
            MessageContentType,
        )

        conv = AnalystConversation(semantic_model="@DB.SCHEMA.STAGE/model.yaml")

        # Add user message
        conv.add_user_message("What is revenue?")
        assert len(conv.messages) == 1

        # Add analyst response
        response = AnalystResponse(
            request_id="req-123",
            message=AnalystMessage(
                role="analyst",
                content=[
                    MessageContent(type=MessageContentType.TEXT, text="Here is your answer"),
                    MessageContent(type=MessageContentType.SQL, statement="SELECT SUM(revenue) FROM sales"),
                ],
            ),
            sql="SELECT SUM(revenue) FROM sales",
            explanation="Here is your answer",
        )
        conv.add_analyst_response(response)

        assert len(conv.messages) == 2
        assert conv.last_sql == "SELECT SUM(revenue) FROM sales"

    def test_query_result(self):
        """Test QueryResult model."""
        from src.cortex_agent.analyst_types import QueryResult

        result = QueryResult(
            sql="SELECT * FROM sales",
            rows=[{"id": 1, "amount": 100}],
            columns=["id", "amount"],
            row_count=1,
            execution_time_ms=50,
        )
        assert result.row_count == 1
        assert not result.truncated


# =============================================================================
# Semantic Model Manager Tests
# =============================================================================

class TestSemanticModelManager:
    """Test SemanticModelManager operations."""

    def test_create_model(self, model_manager):
        """Test creating a semantic model."""
        model = model_manager.create_model(
            name="test_model",
            description="Test model",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        assert model.name == "test_model"
        assert model.database == "ANALYTICS"
        assert model.tables == []

    def test_create_duplicate_model_fails(self, model_manager):
        """Test that creating a duplicate model raises error."""
        model_manager.create_model(
            name="dup_model",
            description="First",
            database="DB",
            schema_name="SCHEMA",
        )

        with pytest.raises(ValueError, match="already exists"):
            model_manager.create_model(
                name="dup_model",
                description="Second",
                database="DB",
                schema_name="SCHEMA",
            )

    def test_list_models(self, model_manager):
        """Test listing models."""
        model_manager.create_model(
            name="model1",
            description="Model 1",
            database="DB",
            schema_name="SCHEMA",
        )
        model_manager.create_model(
            name="model2",
            description="Model 2",
            database="DB",
            schema_name="SCHEMA",
        )

        models = model_manager.list_models()
        assert len(models) == 2
        names = {m["name"] for m in models}
        assert names == {"model1", "model2"}

    def test_delete_model(self, model_manager):
        """Test deleting a model."""
        model_manager.create_model(
            name="to_delete",
            description="Delete me",
            database="DB",
            schema_name="SCHEMA",
        )

        assert model_manager.delete_model("to_delete") is True
        assert model_manager.get_model("to_delete") is None
        assert model_manager.delete_model("nonexistent") is False

    def test_add_table(self, model_manager):
        """Test adding a table to a model."""
        model_manager.create_model(
            name="with_table",
            description="Model with table",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        table = model_manager.add_table(
            model_name="with_table",
            table_name="sales",
            description="Sales data",
            base_database="ANALYTICS",
            base_schema="PUBLIC",
            base_table="SALES_FACT",
            dimensions=[
                {"name": "region", "description": "Region", "expr": "region_name", "data_type": "VARCHAR"},
            ],
            metrics=[
                {"name": "revenue", "description": "Revenue", "expr": "SUM(amount)", "data_type": "NUMBER"},
            ],
        )

        assert table.name == "sales"
        assert len(table.dimensions) == 1
        assert len(table.metrics) == 1

        model = model_manager.get_model("with_table")
        assert len(model.tables) == 1

    def test_add_relationship(self, model_manager):
        """Test adding a relationship between tables."""
        model_manager.create_model(
            name="with_rel",
            description="Model with relationships",
            database="DB",
            schema_name="SCHEMA",
        )

        model_manager.add_table(
            model_name="with_rel",
            table_name="sales",
            description="Sales",
            base_database="DB",
            base_schema="SCHEMA",
            base_table="SALES",
        )

        model_manager.add_table(
            model_name="with_rel",
            table_name="products",
            description="Products",
            base_database="DB",
            base_schema="SCHEMA",
            base_table="PRODUCTS",
        )

        rel = model_manager.add_relationship(
            model_name="with_rel",
            left_table="sales",
            right_table="products",
            columns=[{"left_column": "product_id", "right_column": "id"}],
            join_type="left_outer",
            relationship_type="many_to_one",
        )

        assert rel.left_table == "sales"
        assert rel.right_table == "products"

        model = model_manager.get_model("with_rel")
        assert len(model.relationships) == 1

    def test_generate_yaml(self, model_manager):
        """Test YAML generation."""
        model_manager.create_model(
            name="yaml_test",
            description="YAML test model",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        model_manager.add_table(
            model_name="yaml_test",
            table_name="sales",
            description="Sales data",
            base_database="ANALYTICS",
            base_schema="PUBLIC",
            base_table="SALES_FACT",
            dimensions=[
                {"name": "region", "description": "Region", "expr": "region_name", "data_type": "VARCHAR"},
            ],
            metrics=[
                {"name": "revenue", "description": "Revenue", "expr": "SUM(amount)", "data_type": "NUMBER"},
            ],
        )

        yaml_content = model_manager.generate_yaml("yaml_test")

        assert "name: yaml_test" in yaml_content
        assert "tables:" in yaml_content
        assert "dimensions:" in yaml_content
        assert "metrics:" in yaml_content
        assert "SUM(amount)" in yaml_content

    def test_validate_model_empty(self, model_manager):
        """Test validation of empty model."""
        model_manager.create_model(
            name="empty",
            description="Empty model",
            database="DB",
            schema_name="SCHEMA",
        )

        result = model_manager.validate_model("empty")
        assert result["valid"] is False
        assert "no tables defined" in str(result["errors"]).lower()

    def test_validate_model_valid(self, model_manager):
        """Test validation of valid model."""
        model_manager.create_model(
            name="valid",
            description="Valid model",
            database="DB",
            schema_name="SCHEMA",
        )

        model_manager.add_table(
            model_name="valid",
            table_name="sales",
            description="Sales",
            base_database="DB",
            base_schema="SCHEMA",
            base_table="SALES",
            dimensions=[
                {"name": "region", "description": "Region", "expr": "region", "data_type": "VARCHAR"},
            ],
        )

        result = model_manager.validate_model("valid")
        assert result["valid"] is True
        assert len(result["errors"]) == 0

    def test_persistence(self, temp_data_dir):
        """Test that models persist across manager instances."""
        from src.cortex_agent.semantic_model import SemanticModelManager

        # Create and save
        manager1 = SemanticModelManager(data_dir=temp_data_dir)
        manager1.create_model(
            name="persist_test",
            description="Persistence test",
            database="DB",
            schema_name="SCHEMA",
        )

        # Load in new instance
        manager2 = SemanticModelManager(data_dir=temp_data_dir)
        model = manager2.get_model("persist_test")

        assert model is not None
        assert model.name == "persist_test"


# =============================================================================
# Analyst Client Tests
# =============================================================================

class TestAnalystClient:
    """Test AnalystClient API interactions."""

    def test_build_request(self):
        """Test request body building."""
        from src.cortex_agent.analyst_client import AnalystClient
        from src.cortex_agent.analyst_types import AnalystConversation

        client = AnalystClient(
            account="test_account",
            token_func=lambda: "test_token",
        )

        conv = AnalystConversation(semantic_model="@DB.SCHEMA.STAGE/model.yaml")

        request = client._build_request(
            question="What is revenue?",
            model_file="@DB.SCHEMA.STAGE/model.yaml",
            view=None,
            conversation=conv,
        )

        assert "messages" in request
        assert "semantic_model_file" in request
        assert len(request["messages"]) == 1
        assert request["messages"][0]["role"] == "user"

    def test_parse_response_with_sql(self):
        """Test parsing response with SQL."""
        from src.cortex_agent.analyst_client import AnalystClient

        client = AnalystClient(
            account="test_account",
            token_func=lambda: "test_token",
        )

        response = {
            "request_id": "req-123",
            "message": {
                "role": "analyst",
                "content": [
                    {"type": "text", "text": "Here is your query"},
                    {"type": "sql", "statement": "SELECT SUM(revenue) FROM sales"},
                    {"type": "suggestions", "suggestions": ["Try filtering by date", "Add grouping"]},
                ],
            },
        }

        parsed = client._parse_response(response, "conv-123")

        assert parsed.request_id == "req-123"
        assert parsed.sql == "SELECT SUM(revenue) FROM sales"
        assert parsed.explanation == "Here is your query"
        assert len(parsed.suggestions) == 2
        assert parsed.success is True

    def test_parse_response_with_error(self):
        """Test parsing error response."""
        from src.cortex_agent.analyst_client import AnalystClient

        client = AnalystClient(
            account="test_account",
            token_func=lambda: "test_token",
        )

        response = {
            "request_id": "req-456",
            "message": {
                "role": "analyst",
                "content": [
                    {"type": "error", "message": "Could not understand query"},
                ],
            },
        }

        parsed = client._parse_response(response, "conv-123")

        assert parsed.request_id == "req-456"
        assert parsed.sql is None
        assert parsed.success is False

    def test_conversation_management(self):
        """Test conversation listing and clearing."""
        from src.cortex_agent.analyst_client import AnalystClient
        from src.cortex_agent.analyst_types import AnalystConversation

        client = AnalystClient(
            account="test_account",
            token_func=lambda: "test_token",
        )

        # Create conversations manually
        conv1 = AnalystConversation(semantic_model="model1.yaml")
        conv2 = AnalystConversation(semantic_model="model2.yaml")
        client._conversations[conv1.id] = conv1
        client._conversations[conv2.id] = conv2

        # List
        convs = client.list_conversations()
        assert len(convs) == 2

        # Clear one
        assert client.clear_conversation(conv1.id) is True
        assert len(client.list_conversations()) == 1

        # Clear nonexistent
        assert client.clear_conversation("nonexistent") is False


# =============================================================================
# Integration Tests
# =============================================================================

class TestAnalystIntegration:
    """Integration tests for Cortex Analyst."""

    def test_end_to_end_model_creation(self, temp_data_dir):
        """Test creating a complete model and generating YAML."""
        from src.cortex_agent.semantic_model import SemanticModelManager

        manager = SemanticModelManager(data_dir=temp_data_dir)

        # Create model
        manager.create_model(
            name="e2e_test",
            description="End-to-end test model",
            database="ANALYTICS",
            schema_name="PUBLIC",
        )

        # Add sales table
        manager.add_table(
            model_name="e2e_test",
            table_name="sales",
            description="Sales transactions",
            base_database="ANALYTICS",
            base_schema="PUBLIC",
            base_table="SALES_FACT",
            dimensions=[
                {"name": "region", "synonyms": ["area"], "description": "Sales region", "expr": "REGION_NAME", "data_type": "VARCHAR"},
                {"name": "product_category", "description": "Product category", "expr": "CATEGORY", "data_type": "VARCHAR"},
            ],
            time_dimensions=[
                {"name": "sale_date", "description": "Date of sale", "expr": "SALE_DATE", "data_type": "DATE"},
            ],
            metrics=[
                {"name": "total_revenue", "synonyms": ["revenue"], "description": "Total revenue", "expr": "SUM(AMOUNT)", "data_type": "NUMBER"},
                {"name": "order_count", "description": "Number of orders", "expr": "COUNT(*)", "data_type": "NUMBER"},
            ],
        )

        # Add products table
        manager.add_table(
            model_name="e2e_test",
            table_name="products",
            description="Product dimension",
            base_database="ANALYTICS",
            base_schema="PUBLIC",
            base_table="DIM_PRODUCT",
            dimensions=[
                {"name": "product_name", "description": "Product name", "expr": "NAME", "data_type": "VARCHAR"},
            ],
        )

        # Add relationship
        manager.add_relationship(
            model_name="e2e_test",
            left_table="sales",
            right_table="products",
            columns=[{"left_column": "PRODUCT_ID", "right_column": "ID"}],
        )

        # Validate
        validation = manager.validate_model("e2e_test")
        assert validation["valid"] is True

        # Generate YAML
        yaml_content = manager.generate_yaml("e2e_test")
        assert "e2e_test" in yaml_content
        assert "sales" in yaml_content
        assert "products" in yaml_content
        assert "relationships:" in yaml_content

    def test_model_with_hierarchy_service_mock(self, temp_data_dir):
        """Test generating model from mocked hierarchy service."""
        from src.cortex_agent.semantic_model import SemanticModelManager

        manager = SemanticModelManager(data_dir=temp_data_dir)

        # Mock hierarchy service
        mock_service = MagicMock()
        mock_service.get_project.return_value = {
            "id": "proj-123",
            "name": "Test P&L",
            "default_database": "ANALYTICS",
            "default_schema": "FINANCE",
        }
        mock_service.list_hierarchies.return_value = [
            {
                "hierarchy_id": "REVENUE_1",
                "name": "Revenue",
                "levels": {"LEVEL_1": "Income", "LEVEL_2": "Revenue"},
            },
        ]
        mock_service.get_source_mappings.return_value = [
            {
                "source_database": "ANALYTICS",
                "source_schema": "FINANCE",
                "source_table": "GL_ACCOUNTS",
                "source_column": "ACCOUNT_CODE",
            },
        ]
        mock_service.list_formula_groups.return_value = []

        # Generate model
        model = manager.from_hierarchy_project(
            project_id="proj-123",
            hierarchy_service=mock_service,
            model_name="test_hierarchy_model",
        )

        assert model.name == "test_hierarchy_model"
        assert len(model.tables) >= 1


# =============================================================================
# MCP Tools Tests
# =============================================================================

class TestAnalystMCPTools:
    """Test MCP tool functions."""

    def test_tools_registration(self, temp_data_dir):
        """Test that tools register successfully."""
        from unittest.mock import MagicMock

        mock_mcp = MagicMock()
        mock_mcp.tool = MagicMock(return_value=lambda f: f)

        mock_settings = MagicMock()
        mock_settings.data_dir = temp_data_dir

        from src.cortex_agent.analyst_tools import register_analyst_tools

        result = register_analyst_tools(mock_mcp, mock_settings)

        assert result["tools_registered"] == 14  # Added cortex_bootstrap_semantic_model
        assert "model_management" in result["categories"]
        assert "natural_language" in result["categories"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
