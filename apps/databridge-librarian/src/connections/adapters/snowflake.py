"""
Snowflake database adapter implementation.

Provides connection management, schema introspection, and query execution
for Snowflake data warehouses using the snowflake-connector-python library.
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from ..base import (
    AbstractDatabaseAdapter,
    ColumnInfo,
    QueryResult,
    TableInfo,
)


class SnowflakeAdapter(AbstractDatabaseAdapter):
    """
    Snowflake database adapter.

    Implements the AbstractDatabaseAdapter interface for Snowflake,
    providing connection management, schema introspection, and query execution.

    Example:
        ```python
        adapter = SnowflakeAdapter(
            host="myaccount.snowflakecomputing.com",  # or just "myaccount"
            database="ANALYTICS",
            username="user",
            password="pass",
            extra_config={
                "warehouse": "COMPUTE_WH",
                "role": "ANALYST",
                "schema": "PUBLIC",
            }
        )

        with adapter:
            databases = adapter.list_databases()
            result = adapter.execute_query("SELECT * FROM my_table LIMIT 10")
        ```
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
        """
        Initialize Snowflake adapter.

        Args:
            host: Snowflake account identifier (e.g., "myaccount" or
                  "myaccount.snowflakecomputing.com").
            port: Not used for Snowflake (standard HTTPS port).
            database: Default database to use.
            username: Snowflake username.
            password: Snowflake password.
            extra_config: Additional config including:
                - warehouse: Compute warehouse name
                - role: User role
                - schema: Default schema
                - private_key_path: Path to private key for key-pair auth
                - authenticator: Authentication method (externalbrowser, oauth, etc.)
        """
        super().__init__(host, port, database, username, password, extra_config)
        self._cursor = None

    @property
    def adapter_type(self) -> str:
        """Return the adapter type identifier."""
        return "snowflake"

    @property
    def account(self) -> str:
        """
        Extract account identifier from host.

        Handles both formats:
        - "myaccount" -> "myaccount"
        - "myaccount.snowflakecomputing.com" -> "myaccount"
        - "myaccount.us-east-1.aws" -> "myaccount.us-east-1.aws"
        """
        if not self.host:
            return ""
        # Remove .snowflakecomputing.com if present
        if ".snowflakecomputing.com" in self.host:
            return self.host.replace(".snowflakecomputing.com", "")
        return self.host

    def connect(self) -> bool:
        """
        Establish connection to Snowflake.

        Returns:
            bool: True if connection successful.

        Raises:
            ConnectionError: If connection fails.
            ImportError: If snowflake-connector-python is not installed.
        """
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake connections. "
                "Install with: pip install 'databridge-librarian[connectors]'"
            )

        try:
            connect_params = {
                "account": self.account,
                "user": self.username,
                "database": self.database,
            }

            # Add password if provided
            if self.password:
                connect_params["password"] = self.password

            # Add warehouse if specified
            if self.extra_config.get("warehouse"):
                connect_params["warehouse"] = self.extra_config["warehouse"]

            # Add role if specified
            if self.extra_config.get("role"):
                connect_params["role"] = self.extra_config["role"]

            # Add schema if specified
            if self.extra_config.get("schema"):
                connect_params["schema"] = self.extra_config["schema"]

            # Handle key-pair authentication
            if self.extra_config.get("private_key_path"):
                from cryptography.hazmat.backends import default_backend
                from cryptography.hazmat.primitives import serialization

                with open(self.extra_config["private_key_path"], "rb") as key_file:
                    p_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=self.extra_config.get("private_key_passphrase", "").encode()
                        if self.extra_config.get("private_key_passphrase")
                        else None,
                        backend=default_backend(),
                    )
                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                connect_params["private_key"] = pkb
                # Remove password for key-pair auth
                connect_params.pop("password", None)

            # Handle external browser authentication
            if self.extra_config.get("authenticator"):
                connect_params["authenticator"] = self.extra_config["authenticator"]
                # External browser doesn't need password
                if self.extra_config["authenticator"] == "externalbrowser":
                    connect_params.pop("password", None)

            self._connection = snowflake.connector.connect(**connect_params)
            self._cursor = self._connection.cursor()
            return True

        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {e}")

    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None

        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test the Snowflake connection.

        Returns:
            Tuple of (success, message).
        """
        try:
            was_connected = self._connection is not None

            if not was_connected:
                self.connect()

            # Run a simple query to verify connection
            self._cursor.execute("SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_USER()")
            row = self._cursor.fetchone()

            version = row[0] if row else "unknown"
            account = row[1] if row else "unknown"
            user = row[2] if row else "unknown"

            if not was_connected:
                self.disconnect()

            return True, f"Connected to Snowflake {version} (account: {account}, user: {user})"

        except Exception as e:
            return False, f"Connection failed: {e}"

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
            params: Query parameters (for parameterized queries).
            max_rows: Maximum rows to return.

        Returns:
            QueryResult with columns, rows, and metadata.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        start_time = time.time()

        try:
            if params:
                self._cursor.execute(query, params)
            else:
                self._cursor.execute(query)

            # Get column names from cursor description
            columns = [desc[0] for desc in self._cursor.description] if self._cursor.description else []

            # Fetch rows up to max_rows + 1 to detect truncation
            rows = self._cursor.fetchmany(max_rows + 1)
            truncated = len(rows) > max_rows
            if truncated:
                rows = rows[:max_rows]

            # Convert to tuples
            rows = [tuple(row) for row in rows]

            execution_time_ms = (time.time() - start_time) * 1000

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                execution_time_ms=execution_time_ms,
                truncated=truncated,
            )

        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")

    def list_databases(self) -> List[str]:
        """
        List all accessible databases.

        Returns:
            List of database names.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        self._cursor.execute("SHOW DATABASES")
        rows = self._cursor.fetchall()

        # Database name is typically in the 'name' column (index 1)
        # SHOW DATABASES columns: created_on, name, is_default, is_current, origin, owner, comment, options, retention_time
        return [row[1] for row in rows if row[1]]

    def list_schemas(self, database: Optional[str] = None) -> List[str]:
        """
        List all schemas in a database.

        Args:
            database: Database name. Uses current if not specified.

        Returns:
            List of schema names.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        db = database or self.database
        if db:
            self._cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
        else:
            self._cursor.execute("SHOW SCHEMAS")

        rows = self._cursor.fetchall()

        # Schema name is in the 'name' column (index 1)
        # SHOW SCHEMAS columns: created_on, name, is_default, is_current, database_name, owner, comment, options, retention_time
        return [row[1] for row in rows if row[1]]

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
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        db = database or self.database or ""
        sch = schema or self.extra_config.get("schema", "PUBLIC")

        # Build qualified path
        if db and sch:
            self._cursor.execute(f"SHOW TABLES IN {db}.{sch}")
        elif sch:
            self._cursor.execute(f"SHOW TABLES IN SCHEMA {sch}")
        else:
            self._cursor.execute("SHOW TABLES")

        rows = self._cursor.fetchall()
        tables = []

        # SHOW TABLES columns: created_on, name, database_name, schema_name, kind, comment, cluster_by, rows, bytes, owner, retention_time
        for row in rows:
            tables.append(
                TableInfo(
                    name=row[1],  # name
                    schema=row[3],  # schema_name
                    database=row[2],  # database_name
                    table_type=row[4] if len(row) > 4 else "TABLE",  # kind
                    row_count=row[7] if len(row) > 7 and row[7] else None,  # rows
                )
            )

        # Also get views
        if db and sch:
            self._cursor.execute(f"SHOW VIEWS IN {db}.{sch}")
        elif sch:
            self._cursor.execute(f"SHOW VIEWS IN SCHEMA {sch}")
        else:
            self._cursor.execute("SHOW VIEWS")

        view_rows = self._cursor.fetchall()
        for row in view_rows:
            tables.append(
                TableInfo(
                    name=row[1],  # name
                    schema=row[3] if len(row) > 3 else sch,  # schema_name
                    database=row[2] if len(row) > 2 else db,  # database_name
                    table_type="VIEW",
                    row_count=None,
                )
            )

        return tables

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
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        db = database or self.database or ""
        sch = schema or self.extra_config.get("schema", "PUBLIC")

        # Build fully qualified table name
        if db and sch:
            qualified_table = f"{db}.{sch}.{table}"
        elif sch:
            qualified_table = f"{sch}.{table}"
        else:
            qualified_table = table

        self._cursor.execute(f"DESCRIBE TABLE {qualified_table}")
        rows = self._cursor.fetchall()

        columns = []
        # DESCRIBE TABLE columns: name, type, kind, null?, default, primary key, unique key, check, expression, comment, policy name
        for row in rows:
            col_name = row[0]
            data_type = row[1]
            nullable = row[3] == "Y" if len(row) > 3 else True
            default_val = row[4] if len(row) > 4 else None
            is_pk = row[5] == "Y" if len(row) > 5 else False

            columns.append(
                ColumnInfo(
                    name=col_name,
                    data_type=data_type,
                    nullable=nullable,
                    default=default_val,
                    is_primary_key=is_pk,
                    extra={
                        "kind": row[2] if len(row) > 2 else None,
                        "unique": row[6] == "Y" if len(row) > 6 else False,
                        "comment": row[9] if len(row) > 9 else None,
                    },
                )
            )

        return columns

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
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        db = database or self.database or ""
        sch = schema or self.extra_config.get("schema", "PUBLIC")

        # Build fully qualified table name
        if db and sch:
            qualified_table = f"{db}.{sch}.{table}"
        elif sch:
            qualified_table = f"{sch}.{table}"
        else:
            qualified_table = table

        # Use parameterized column reference isn't possible for column names,
        # but we validate the column exists first for safety
        columns = self.list_columns(table, database, schema)
        column_names = [c.name.upper() for c in columns]
        if column.upper() not in column_names:
            raise ValueError(f"Column '{column}' not found in table '{table}'")

        query = f'SELECT DISTINCT "{column}" FROM {qualified_table} LIMIT {limit}'
        self._cursor.execute(query)
        rows = self._cursor.fetchall()

        return [row[0] for row in rows]

    def get_connection_string(self) -> str:
        """
        Get a SQLAlchemy connection string for Snowflake.

        Returns:
            Connection string for SQLAlchemy.
        """
        warehouse = self.extra_config.get("warehouse", "COMPUTE_WH")
        schema = self.extra_config.get("schema", "PUBLIC")
        role = self.extra_config.get("role", "")

        # Build SQLAlchemy Snowflake connection string
        # Format: snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}
        conn_str = f"snowflake://{self.username}:{self.password}@{self.account}"

        if self.database:
            conn_str += f"/{self.database}"
            if schema:
                conn_str += f"/{schema}"

        params = []
        if warehouse:
            params.append(f"warehouse={warehouse}")
        if role:
            params.append(f"role={role}")

        if params:
            conn_str += "?" + "&".join(params)

        return conn_str

    def get_current_context(self) -> Dict[str, str]:
        """
        Get current Snowflake context (database, schema, warehouse, role).

        Returns:
            Dictionary with current context values.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        self._cursor.execute("""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE(),
                CURRENT_ROLE(),
                CURRENT_USER(),
                CURRENT_ACCOUNT()
        """)
        row = self._cursor.fetchone()

        return {
            "database": row[0] if row else None,
            "schema": row[1] if row else None,
            "warehouse": row[2] if row else None,
            "role": row[3] if row else None,
            "user": row[4] if row else None,
            "account": row[5] if row else None,
        }

    def use_database(self, database: str) -> None:
        """
        Switch to a different database.

        Args:
            database: Database name to switch to.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        self._cursor.execute(f"USE DATABASE {database}")
        self.database = database

    def use_schema(self, schema: str) -> None:
        """
        Switch to a different schema.

        Args:
            schema: Schema name to switch to.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        self._cursor.execute(f"USE SCHEMA {schema}")
        self.extra_config["schema"] = schema

    def use_warehouse(self, warehouse: str) -> None:
        """
        Switch to a different warehouse.

        Args:
            warehouse: Warehouse name to switch to.
        """
        if not self._connection or not self._cursor:
            raise RuntimeError("Not connected to Snowflake. Call connect() first.")

        self._cursor.execute(f"USE WAREHOUSE {warehouse}")
        self.extra_config["warehouse"] = warehouse
