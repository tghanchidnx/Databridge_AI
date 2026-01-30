"""
Unit tests for PostgreSQL connector.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock


class TestPostgreSQLConnector:
    """Tests for PostgreSQLConnector class."""

    def test_connector_type(self):
        """Test that connector type is PostgreSQL."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.postgresql import PostgreSQLConnector
                from src.connectors.base import ConnectorType

                connector = PostgreSQLConnector(
                    name="test",
                    host="localhost",
                    database="test_db",
                )

                assert connector.connector_type == ConnectorType.POSTGRESQL

    def test_default_values(self):
        """Test default connection values."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.postgresql import PostgreSQLConnector

                connector = PostgreSQLConnector(name="test")

                assert connector.host == "localhost"
                assert connector.port == 5432
                assert connector.database == "postgres"
                assert connector.user == "postgres"
                assert connector.schema == "public"

    def test_is_connected_initially_false(self):
        """Test that is_connected is False initially."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.postgresql import PostgreSQLConnector

                connector = PostgreSQLConnector(name="test")

                assert connector.is_connected is False

    @patch("src.connectors.postgresql.psycopg2")
    def test_connect_success(self, mock_psycopg2):
        """Test successful connection."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_psycopg2.connect.return_value = mock_connection

        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(
                name="test",
                host="localhost",
                database="test_db",
                user="test_user",
                password="test_pass",
            )

            status = connector.connect()

            assert status.connected is True
            assert connector.is_connected is True
            mock_psycopg2.connect.assert_called_once()

    @patch("src.connectors.postgresql.psycopg2")
    def test_connect_failure(self, mock_psycopg2):
        """Test failed connection."""
        import psycopg2
        mock_psycopg2.Error = psycopg2.Error
        mock_psycopg2.connect.side_effect = psycopg2.Error("Connection refused")

        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(name="test")

            status = connector.connect()

            assert status.connected is False
            assert "Connection refused" in status.error

    @patch("src.connectors.postgresql.psycopg2")
    def test_disconnect(self, mock_psycopg2):
        """Test disconnection."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_psycopg2.connect.return_value = mock_connection

        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(name="test")
            connector.connect()
            connector.disconnect()

            assert connector.is_connected is False
            mock_connection.close.assert_called_once()

    @patch("src.connectors.postgresql.psycopg2")
    def test_execute_not_connected_raises(self, mock_psycopg2):
        """Test that execute raises when not connected."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(name="test")

            with pytest.raises(ConnectionError, match="Not connected"):
                connector.execute("SELECT 1")

    @patch("src.connectors.postgresql.psycopg2")
    def test_execute_query(self, mock_psycopg2):
        """Test query execution."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "foo"), (2, "bar")]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.notices = []
        mock_psycopg2.connect.return_value = mock_connection

        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(name="test")
            connector.connect()

            result = connector.execute("SELECT id, name FROM test")

            assert result.columns == ["id", "name"]
            assert result.row_count == 2
            assert result.rows == [(1, "foo"), (2, "bar")]

    @patch("src.connectors.postgresql.psycopg2")
    def test_execute_with_limit(self, mock_psycopg2):
        """Test query execution with limit."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connection.notices = []
        mock_psycopg2.connect.return_value = mock_connection

        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            from src.connectors.postgresql import PostgreSQLConnector

            connector = PostgreSQLConnector(name="test")
            connector.connect()

            connector.execute("SELECT id FROM test", limit=10)

            # Verify LIMIT was added
            call_args = mock_cursor.execute.call_args[0][0]
            assert "LIMIT 10" in call_args

    def test_quote_identifier(self):
        """Test PostgreSQL identifier quoting."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.postgresql import PostgreSQLConnector

                connector = PostgreSQLConnector(name="test")

                assert connector._quote_identifier("table_name") == '"table_name"'
                assert connector._quote_identifier('with"quote') == '"with""quote"'


class TestConnectorFactory:
    """Tests for ConnectorFactory."""

    def test_create_postgresql_connector(self):
        """Test creating PostgreSQL connector via factory."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.factory import ConnectorFactory
                from src.connectors.base import ConnectorType

                connector = ConnectorFactory.create(
                    ConnectorType.POSTGRESQL,
                    name="test_pg",
                    host="db.example.com",
                    database="analytics",
                )

                assert connector.name == "test_pg"
                assert connector.host == "db.example.com"
                assert connector.database == "analytics"

    def test_create_from_string_type(self):
        """Test creating connector from string type."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.factory import ConnectorFactory

                connector = ConnectorFactory.create(
                    "postgresql",
                    name="test",
                )

                assert connector.connector_type.value == "postgresql"

    def test_create_from_dict(self):
        """Test creating connector from dictionary."""
        with patch("src.connectors.postgresql.PSYCOPG2_AVAILABLE", True):
            with patch("src.connectors.postgresql.psycopg2"):
                from src.connectors.factory import ConnectorFactory

                config = {
                    "type": "postgresql",
                    "name": "test_from_dict",
                    "host": "localhost",
                    "port": 5433,
                    "database": "mydb",
                }

                connector = ConnectorFactory.create_from_dict(config)

                assert connector.name == "test_from_dict"
                assert connector.port == 5433
                assert connector.database == "mydb"

    def test_create_from_dict_missing_type_raises(self):
        """Test that missing type in dict raises error."""
        from src.connectors.factory import ConnectorFactory

        with pytest.raises(ValueError, match="must include 'type'"):
            ConnectorFactory.create_from_dict({"name": "test"})

    def test_unsupported_type_raises(self):
        """Test that unsupported type raises error."""
        from src.connectors.factory import ConnectorFactory

        with pytest.raises(ValueError, match="is not a valid ConnectorType"):
            ConnectorFactory.create("unsupported_db", name="test")
