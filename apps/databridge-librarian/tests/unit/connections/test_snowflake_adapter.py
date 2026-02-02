"""
Unit tests for Snowflake database adapter.

These tests mock the snowflake-connector-python library to test
the adapter logic without requiring an actual Snowflake connection.
"""

import sys
import pytest
from unittest.mock import MagicMock, patch

from src.connections.adapters.snowflake import SnowflakeAdapter
from src.connections.base import ColumnInfo, QueryResult, TableInfo


@pytest.fixture
def mock_snowflake():
    """Create mock snowflake module."""
    mock_module = MagicMock()
    mock_connector = MagicMock()
    mock_module.connector = mock_connector
    return mock_module, mock_connector


@pytest.fixture
def connected_adapter(mock_snowflake):
    """Create an adapter with mocked connection."""
    mock_module, mock_connector = mock_snowflake
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connector.connect.return_value = mock_conn

    with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
        adapter = SnowflakeAdapter(
            host="myaccount",
            database="ANALYTICS",
            username="user",
            password="pass",
            extra_config={
                "warehouse": "COMPUTE_WH",
                "role": "ANALYST",
                "schema": "PUBLIC",
            },
        )
        adapter.connect()
        yield adapter, mock_cursor


class TestSnowflakeAdapterInit:
    """Test SnowflakeAdapter initialization."""

    def test_init_basic(self):
        """Test basic initialization."""
        adapter = SnowflakeAdapter(
            host="myaccount",
            database="ANALYTICS",
            username="user",
            password="pass",
        )
        assert adapter.host == "myaccount"
        assert adapter.database == "ANALYTICS"
        assert adapter.username == "user"
        assert adapter.password == "pass"
        assert adapter.adapter_type == "snowflake"

    def test_init_with_extra_config(self):
        """Test initialization with extra config."""
        adapter = SnowflakeAdapter(
            host="myaccount.snowflakecomputing.com",
            database="ANALYTICS",
            username="user",
            password="pass",
            extra_config={
                "warehouse": "COMPUTE_WH",
                "role": "ANALYST",
                "schema": "PUBLIC",
            },
        )
        assert adapter.extra_config["warehouse"] == "COMPUTE_WH"
        assert adapter.extra_config["role"] == "ANALYST"
        assert adapter.extra_config["schema"] == "PUBLIC"

    def test_account_extraction_simple(self):
        """Test account extraction from simple host."""
        adapter = SnowflakeAdapter(host="myaccount")
        assert adapter.account == "myaccount"

    def test_account_extraction_full_url(self):
        """Test account extraction from full Snowflake URL."""
        adapter = SnowflakeAdapter(host="myaccount.snowflakecomputing.com")
        assert adapter.account == "myaccount"

    def test_account_extraction_with_region(self):
        """Test account extraction with region identifier."""
        adapter = SnowflakeAdapter(host="myaccount.us-east-1.aws")
        assert adapter.account == "myaccount.us-east-1.aws"


class TestSnowflakeAdapterConnection:
    """Test connection management."""

    def test_connect_success(self, mock_snowflake):
        """Test successful connection."""
        mock_module, mock_connector = mock_snowflake
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn

        with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            adapter = SnowflakeAdapter(
                host="myaccount",
                database="ANALYTICS",
                username="user",
                password="pass",
                extra_config={
                    "warehouse": "COMPUTE_WH",
                    "role": "ANALYST",
                },
            )

            result = adapter.connect()

            assert result is True
            assert adapter._connection is mock_conn
            assert adapter._cursor is mock_cursor
            mock_connector.connect.assert_called_once()

    def test_connect_failure(self, mock_snowflake):
        """Test connection failure."""
        mock_module, mock_connector = mock_snowflake
        mock_connector.connect.side_effect = Exception("Connection refused")

        with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            adapter = SnowflakeAdapter(
                host="myaccount",
                database="ANALYTICS",
                username="user",
                password="wrong_pass",
            )

            with pytest.raises(ConnectionError) as exc_info:
                adapter.connect()

            assert "Failed to connect to Snowflake" in str(exc_info.value)

    def test_disconnect(self, mock_snowflake):
        """Test disconnection."""
        mock_module, mock_connector = mock_snowflake
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn

        with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            adapter = SnowflakeAdapter(host="myaccount", username="user", password="pass")
            adapter.connect()
            adapter.disconnect()

            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()
            assert adapter._connection is None
            assert adapter._cursor is None

    def test_test_connection_success(self, mock_snowflake):
        """Test connection test."""
        mock_module, mock_connector = mock_snowflake
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("7.0.0", "MYACCOUNT", "TESTUSER")
        mock_connector.connect.return_value = mock_conn

        with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            adapter = SnowflakeAdapter(host="myaccount", username="user", password="pass")

            success, message = adapter.test_connection()

            assert success is True
            assert "Connected to Snowflake 7.0.0" in message
            assert "MYACCOUNT" in message

    def test_context_manager(self, mock_snowflake):
        """Test context manager usage."""
        mock_module, mock_connector = mock_snowflake
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connector.connect.return_value = mock_conn

        with patch.dict(sys.modules, {"snowflake": mock_module, "snowflake.connector": mock_connector}):
            adapter = SnowflakeAdapter(host="myaccount", username="user", password="pass")

            with adapter:
                assert adapter._connection is not None

            mock_conn.close.assert_called_once()


class TestSnowflakeAdapterQueries:
    """Test query execution."""

    def test_execute_query_basic(self, connected_adapter):
        """Test basic query execution."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.description = [("COL1",), ("COL2",)]
        mock_cursor.fetchmany.return_value = [
            ("val1", "val2"),
            ("val3", "val4"),
        ]

        result = adapter.execute_query("SELECT * FROM test_table")

        assert isinstance(result, QueryResult)
        assert result.columns == ["COL1", "COL2"]
        assert len(result.rows) == 2
        assert result.truncated is False

    def test_execute_query_truncated(self, connected_adapter):
        """Test query result truncation."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.description = [("COL1",)]
        # Return more than max_rows
        mock_cursor.fetchmany.return_value = [("val1",), ("val2",), ("val3",)]

        result = adapter.execute_query("SELECT * FROM test_table", max_rows=2)

        assert result.truncated is True
        assert len(result.rows) == 2

    def test_execute_query_not_connected(self):
        """Test query without connection raises error."""
        adapter = SnowflakeAdapter(host="myaccount", username="user", password="pass")

        with pytest.raises(RuntimeError) as exc_info:
            adapter.execute_query("SELECT 1")

        assert "Not connected" in str(exc_info.value)


class TestSnowflakeAdapterSchemaIntrospection:
    """Test schema introspection methods."""

    def test_list_databases(self, connected_adapter):
        """Test listing databases."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "DB1", False, True, None, "OWNER", None, None, 1),
            ("2024-01-01", "DB2", False, False, None, "OWNER", None, None, 1),
        ]

        databases = adapter.list_databases()

        assert databases == ["DB1", "DB2"]

    def test_list_schemas(self, connected_adapter):
        """Test listing schemas."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "PUBLIC", False, True, "DB1", "OWNER", None, None, 1),
            ("2024-01-01", "STAGING", False, False, "DB1", "OWNER", None, None, 1),
        ]

        schemas = adapter.list_schemas()

        assert schemas == ["PUBLIC", "STAGING"]

    def test_list_tables(self, connected_adapter):
        """Test listing tables."""
        adapter, mock_cursor = connected_adapter

        # First call returns tables, second returns views
        mock_cursor.fetchall.side_effect = [
            [
                ("2024-01-01", "TABLE1", "DB1", "PUBLIC", "TABLE", None, None, 1000, 5000, "OWNER", 1),
                ("2024-01-01", "TABLE2", "DB1", "PUBLIC", "TABLE", None, None, 500, 2500, "OWNER", 1),
            ],
            [
                ("2024-01-01", "VIEW1", "DB1", "PUBLIC", None, None, None, None, None, "OWNER", 1),
            ],
        ]

        tables = adapter.list_tables()

        assert len(tables) == 3
        assert any(t.name == "TABLE1" and t.table_type == "TABLE" for t in tables)
        assert any(t.name == "VIEW1" and t.table_type == "VIEW" for t in tables)

    def test_list_columns(self, connected_adapter):
        """Test listing columns."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.fetchall.return_value = [
            ("ID", "NUMBER(38,0)", "COLUMN", "N", None, "Y", "N", None, None, "Primary key", None),
            ("NAME", "VARCHAR(100)", "COLUMN", "Y", None, "N", "N", None, None, "Customer name", None),
            ("AMOUNT", "NUMBER(18,2)", "COLUMN", "Y", "0.00", "N", "N", None, None, None, None),
        ]

        columns = adapter.list_columns("CUSTOMERS")

        assert len(columns) == 3
        assert columns[0].name == "ID"
        assert columns[0].is_primary_key is True
        assert columns[0].nullable is False
        assert columns[1].name == "NAME"
        assert columns[1].nullable is True

    def test_get_distinct_values(self, connected_adapter):
        """Test getting distinct values."""
        adapter, mock_cursor = connected_adapter

        # First call for column validation (list_columns), second for distinct values
        mock_cursor.fetchall.side_effect = [
            [("STATUS", "VARCHAR", "COLUMN", "Y", None, "N", "N", None, None, None, None)],
            [("ACTIVE",), ("INACTIVE",), ("PENDING",)],
        ]

        values = adapter.get_distinct_values("CUSTOMERS", "STATUS")

        assert values == ["ACTIVE", "INACTIVE", "PENDING"]


class TestSnowflakeAdapterConnectionString:
    """Test connection string generation."""

    def test_connection_string_basic(self):
        """Test basic connection string."""
        adapter = SnowflakeAdapter(
            host="myaccount",
            database="ANALYTICS",
            username="user",
            password="pass",
            extra_config={"warehouse": "COMPUTE_WH", "schema": "PUBLIC"},
        )

        conn_str = adapter.get_connection_string()

        assert "snowflake://user:pass@myaccount" in conn_str
        assert "ANALYTICS/PUBLIC" in conn_str
        assert "warehouse=COMPUTE_WH" in conn_str

    def test_connection_string_with_role(self):
        """Test connection string with role."""
        adapter = SnowflakeAdapter(
            host="myaccount",
            database="ANALYTICS",
            username="user",
            password="pass",
            extra_config={
                "warehouse": "COMPUTE_WH",
                "role": "ANALYST",
                "schema": "PUBLIC",
            },
        )

        conn_str = adapter.get_connection_string()

        assert "role=ANALYST" in conn_str


class TestSnowflakeAdapterContextOperations:
    """Test context switching operations."""

    def test_get_current_context(self, connected_adapter):
        """Test getting current context."""
        adapter, mock_cursor = connected_adapter
        mock_cursor.fetchone.return_value = (
            "ANALYTICS", "PUBLIC", "COMPUTE_WH", "ANALYST", "USER", "MYACCOUNT"
        )

        context = adapter.get_current_context()

        assert context["database"] == "ANALYTICS"
        assert context["schema"] == "PUBLIC"
        assert context["warehouse"] == "COMPUTE_WH"
        assert context["role"] == "ANALYST"

    def test_use_database(self, connected_adapter):
        """Test switching database."""
        adapter, mock_cursor = connected_adapter

        adapter.use_database("NEW_DB")

        mock_cursor.execute.assert_called_with("USE DATABASE NEW_DB")
        assert adapter.database == "NEW_DB"

    def test_use_schema(self, connected_adapter):
        """Test switching schema."""
        adapter, mock_cursor = connected_adapter

        adapter.use_schema("STAGING")

        mock_cursor.execute.assert_called_with("USE SCHEMA STAGING")
        assert adapter.extra_config["schema"] == "STAGING"

    def test_use_warehouse(self, connected_adapter):
        """Test switching warehouse."""
        adapter, mock_cursor = connected_adapter

        adapter.use_warehouse("LARGE_WH")

        mock_cursor.execute.assert_called_with("USE WAREHOUSE LARGE_WH")
        assert adapter.extra_config["warehouse"] == "LARGE_WH"


@pytest.mark.phase5
class TestSnowflakeAdapterImportError:
    """Test adapter import errors."""

    def test_import_not_installed(self):
        """Test that adapter raises ImportError when snowflake not installed."""
        # Remove snowflake from sys.modules to simulate not installed
        saved_modules = {}
        for key in list(sys.modules.keys()):
            if "snowflake" in key:
                saved_modules[key] = sys.modules.pop(key)

        try:
            # Mock the import to fail
            with patch.dict(sys.modules, {"snowflake": None}):
                adapter = SnowflakeAdapter(
                    host="myaccount", username="user", password="pass"
                )

                with pytest.raises(ImportError) as exc_info:
                    adapter.connect()

                assert "snowflake-connector-python" in str(exc_info.value)
        finally:
            # Restore modules
            sys.modules.update(saved_modules)
