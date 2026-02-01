"""
Base database adapter interface.

All database adapters must implement this abstract base class.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    nullable: bool = True
    default: Optional[str] = None
    is_primary_key: bool = False
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    schema: str
    database: str
    table_type: str = "TABLE"  # TABLE, VIEW, etc.
    row_count: Optional[int] = None


@dataclass
class QueryResult:
    """Result of a database query."""
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    row_count: int
    execution_time_ms: float = 0.0
    truncated: bool = False


class AbstractDatabaseAdapter(ABC):
    """
    Abstract base class for database adapters.

    All database adapters (Snowflake, MySQL, PostgreSQL, SQL Server)
    must implement these methods to provide consistent functionality.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.extra_config = extra_config or {}
        self._connection = None

    @property
    @abstractmethod
    def adapter_type(self) -> str:
        """Return the adapter type identifier (e.g., 'snowflake', 'mysql')."""
        pass

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the database.

        Returns:
            bool: True if connection successful.

        Raises:
            ConnectionError: If connection fails.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the database connection.

        Returns:
            Tuple of (success, message).
        """
        pass

    @abstractmethod
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        max_rows: int = 1000,
    ) -> QueryResult:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query to execute.
            params: Query parameters.
            max_rows: Maximum rows to return.

        Returns:
            QueryResult with columns, rows, and metadata.
        """
        pass

    @abstractmethod
    def list_databases(self) -> List[str]:
        """List all accessible databases."""
        pass

    @abstractmethod
    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """
        List all schemas in a database.

        Args:
            database: Database name. Uses current if not specified.

        Returns:
            List of schema names.
        """
        pass

    @abstractmethod
    def list_tables(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[TableInfo]:
        """
        List all tables in a schema.

        Args:
            database: Database name.
            schema: Schema name.

        Returns:
            List of TableInfo objects.
        """
        pass

    @abstractmethod
    def list_columns(
        self,
        table: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[ColumnInfo]:
        """
        List all columns in a table.

        Args:
            table: Table name.
            database: Database name.
            schema: Schema name.

        Returns:
            List of ColumnInfo objects.
        """
        pass

    @abstractmethod
    def get_distinct_values(
        self,
        table: str,
        column: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 100,
    ) -> List[Any]:
        """
        Get distinct values from a column.

        Args:
            table: Table name.
            column: Column name.
            database: Database name.
            schema: Schema name.
            limit: Maximum values to return.

        Returns:
            List of distinct values.
        """
        pass

    def get_connection_string(self) -> str:
        """
        Get a SQLAlchemy connection string.

        Returns:
            Connection string for SQLAlchemy.
        """
        raise NotImplementedError("Subclass must implement get_connection_string")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
