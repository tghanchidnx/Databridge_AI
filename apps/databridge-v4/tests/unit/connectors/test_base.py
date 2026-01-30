"""
Unit tests for the base connector classes.
"""

import pytest
from dataclasses import asdict


class TestConnectorType:
    """Tests for ConnectorType enum."""

    def test_connector_types_exist(self):
        """Test all expected connector types exist."""
        from src.connectors.base import ConnectorType

        assert ConnectorType.SNOWFLAKE.value == "snowflake"
        assert ConnectorType.DATABRICKS.value == "databricks"
        assert ConnectorType.SQLSERVER.value == "sqlserver"
        assert ConnectorType.POSTGRESQL.value == "postgresql"
        assert ConnectorType.MYSQL.value == "mysql"
        assert ConnectorType.ORACLE.value == "oracle"


class TestConnectionStatus:
    """Tests for ConnectionStatus dataclass."""

    def test_connection_status_success(self):
        """Test successful connection status."""
        from src.connectors.base import ConnectionStatus, ConnectorType
        from datetime import datetime

        status = ConnectionStatus(
            connected=True,
            connector_type=ConnectorType.POSTGRESQL,
            host="localhost",
            database="test_db",
            user="test_user",
            last_connected=datetime.now(),
        )

        assert status.connected is True
        assert status.connector_type == ConnectorType.POSTGRESQL
        assert status.error is None

    def test_connection_status_failure(self):
        """Test failed connection status."""
        from src.connectors.base import ConnectionStatus, ConnectorType

        status = ConnectionStatus(
            connected=False,
            connector_type=ConnectorType.SNOWFLAKE,
            error="Connection refused",
        )

        assert status.connected is False
        assert status.error == "Connection refused"


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_query_result_basic(self):
        """Test basic QueryResult creation."""
        from src.connectors.base import QueryResult

        result = QueryResult(
            columns=["id", "name", "value"],
            rows=[(1, "foo", 100), (2, "bar", 200)],
            row_count=2,
            execution_time_ms=50.5,
            query="SELECT * FROM test",
        )

        assert result.columns == ["id", "name", "value"]
        assert result.row_count == 2
        assert len(result.rows) == 2

    def test_to_dict_rows(self):
        """Test converting rows to dictionaries."""
        from src.connectors.base import QueryResult

        result = QueryResult(
            columns=["id", "name"],
            rows=[(1, "foo"), (2, "bar")],
            row_count=2,
            execution_time_ms=10,
            query="SELECT * FROM test",
        )

        dict_rows = result.to_dict_rows()

        assert len(dict_rows) == 2
        assert dict_rows[0] == {"id": 1, "name": "foo"}
        assert dict_rows[1] == {"id": 2, "name": "bar"}

    def test_get_column(self):
        """Test extracting a single column."""
        from src.connectors.base import QueryResult

        result = QueryResult(
            columns=["id", "value"],
            rows=[(1, 100), (2, 200), (3, 300)],
            row_count=3,
            execution_time_ms=10,
            query="SELECT * FROM test",
        )

        values = result.get_column("value")

        assert values == [100, 200, 300]

    def test_get_column_not_found(self):
        """Test getting non-existent column raises error."""
        from src.connectors.base import QueryResult

        result = QueryResult(
            columns=["id", "value"],
            rows=[(1, 100)],
            row_count=1,
            execution_time_ms=10,
            query="SELECT * FROM test",
        )

        with pytest.raises(ValueError, match="Column 'missing' not found"):
            result.get_column("missing")


class TestTableMetadata:
    """Tests for TableMetadata dataclass."""

    def test_table_metadata_creation(self):
        """Test TableMetadata creation."""
        from src.connectors.base import TableMetadata

        meta = TableMetadata(
            database="analytics",
            schema="public",
            table_name="fact_sales",
            table_type="TABLE",
            row_count=1000000,
            comment="Sales fact table",
        )

        assert meta.database == "analytics"
        assert meta.table_name == "fact_sales"
        assert meta.row_count == 1000000


class TestColumnMetadata:
    """Tests for ColumnMetadata dataclass."""

    def test_column_metadata_creation(self):
        """Test ColumnMetadata creation."""
        from src.connectors.base import ColumnMetadata

        meta = ColumnMetadata(
            database="analytics",
            schema="public",
            table_name="fact_sales",
            column_name="amount",
            data_type="DECIMAL(18,2)",
            ordinal_position=5,
            is_nullable=False,
        )

        assert meta.column_name == "amount"
        assert meta.data_type == "DECIMAL(18,2)"
        assert meta.is_nullable is False

    def test_column_metadata_with_foreign_key(self):
        """Test ColumnMetadata with foreign key reference."""
        from src.connectors.base import ColumnMetadata

        meta = ColumnMetadata(
            database="analytics",
            schema="public",
            table_name="fact_sales",
            column_name="customer_id",
            data_type="INTEGER",
            ordinal_position=2,
            is_foreign_key=True,
            foreign_key_ref="public.dim_customer.id",
        )

        assert meta.is_foreign_key is True
        assert meta.foreign_key_ref == "public.dim_customer.id"


class TestDataWarehouseConnectorAbstract:
    """Tests for abstract base connector."""

    def test_cannot_instantiate_abstract(self):
        """Test that abstract class cannot be instantiated."""
        from src.connectors.base import DataWarehouseConnector

        with pytest.raises(TypeError):
            DataWarehouseConnector("test")

    def test_build_qualified_name(self):
        """Test qualified name building via a mock implementation."""
        from src.connectors.base import DataWarehouseConnector, ConnectorType, ConnectionStatus, QueryResult

        # Create minimal concrete implementation for testing
        class MockConnector(DataWarehouseConnector):
            @property
            def connector_type(self):
                return ConnectorType.POSTGRESQL

            def connect(self):
                return ConnectionStatus(connected=True, connector_type=self.connector_type)

            def disconnect(self):
                pass

            def test_connection(self):
                return self.connect()

            def execute(self, query, params=None, limit=None):
                return QueryResult(columns=[], rows=[], row_count=0, execution_time_ms=0, query=query)

            def get_databases(self):
                return []

            def get_schemas(self, database=None):
                return []

            def get_tables(self, database=None, schema=None, include_views=True):
                return []

            def get_columns(self, table_name, database=None, schema=None):
                return []

        connector = MockConnector("test")

        # Test qualified name building
        name = connector._build_qualified_name("my_table")
        assert name == '"my_table"'

        name = connector._build_qualified_name("my_table", schema="public")
        assert name == '"public"."my_table"'

        name = connector._build_qualified_name("my_table", database="analytics", schema="dbo")
        assert name == '"analytics"."dbo"."my_table"'

    def test_quote_identifier_with_special_chars(self):
        """Test identifier quoting with special characters."""
        from src.connectors.base import DataWarehouseConnector, ConnectorType, ConnectionStatus, QueryResult

        class MockConnector(DataWarehouseConnector):
            @property
            def connector_type(self):
                return ConnectorType.POSTGRESQL

            def connect(self):
                return ConnectionStatus(connected=True, connector_type=self.connector_type)

            def disconnect(self):
                pass

            def test_connection(self):
                return self.connect()

            def execute(self, query, params=None, limit=None):
                return QueryResult(columns=[], rows=[], row_count=0, execution_time_ms=0, query=query)

            def get_databases(self):
                return []

            def get_schemas(self, database=None):
                return []

            def get_tables(self, database=None, schema=None, include_views=True):
                return []

            def get_columns(self, table_name, database=None, schema=None):
                return []

        connector = MockConnector("test")

        # Test with quotes in identifier
        quoted = connector._quote_identifier('table"name')
        assert quoted == '"table""name"'
