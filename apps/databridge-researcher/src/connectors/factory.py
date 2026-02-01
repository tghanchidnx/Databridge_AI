"""
Connector factory for creating data warehouse connectors.

Provides a unified interface for creating connectors based on type.
"""

from typing import Any, Optional

from ..core.config import get_settings
from .base import DataWarehouseConnector, ConnectorType


class ConnectorFactory:
    """
    Factory for creating data warehouse connectors.

    Supports creating connectors from:
    - Configuration settings
    - Connection dictionaries
    - Direct parameters
    """

    _connector_classes: dict[ConnectorType, type] = {}

    @classmethod
    def register(cls, connector_type: ConnectorType, connector_class: type) -> None:
        """
        Register a connector class for a type.

        Args:
            connector_type: The connector type.
            connector_class: The connector class.
        """
        cls._connector_classes[connector_type] = connector_class

    @classmethod
    def create(
        cls,
        connector_type: ConnectorType | str,
        name: str,
        **kwargs: Any,
    ) -> DataWarehouseConnector:
        """
        Create a connector instance.

        Args:
            connector_type: Type of connector to create.
            name: Name for the connection.
            **kwargs: Connector-specific parameters.

        Returns:
            DataWarehouseConnector: Configured connector instance.

        Raises:
            ValueError: If connector type is not supported.
        """
        if isinstance(connector_type, str):
            connector_type = ConnectorType(connector_type.lower())

        connector_class = cls._connector_classes.get(connector_type)

        if connector_class is None:
            # Lazy load connector class
            connector_class = cls._load_connector_class(connector_type)

        if connector_class is None:
            raise ValueError(f"Unsupported connector type: {connector_type}")

        return connector_class(name=name, **kwargs)

    @classmethod
    def create_from_settings(
        cls,
        connector_type: Optional[ConnectorType | str] = None,
        name: Optional[str] = None,
    ) -> DataWarehouseConnector:
        """
        Create a connector from application settings.

        Args:
            connector_type: Type of connector (uses default if None).
            name: Connection name (auto-generated if None).

        Returns:
            DataWarehouseConnector: Configured connector instance.
        """
        settings = get_settings()

        if connector_type is None:
            connector_type = ConnectorType(settings.default_warehouse_type)
        elif isinstance(connector_type, str):
            connector_type = ConnectorType(connector_type.lower())

        name = name or f"{connector_type.value}_default"

        # Get connector-specific settings
        connector_settings = cls._get_settings_for_type(connector_type, settings)

        return cls.create(connector_type, name, **connector_settings)

    @classmethod
    def create_from_dict(cls, config: dict[str, Any]) -> DataWarehouseConnector:
        """
        Create a connector from a configuration dictionary.

        Args:
            config: Configuration dictionary with 'type', 'name', and
                   connector-specific parameters.

        Returns:
            DataWarehouseConnector: Configured connector instance.
        """
        config = config.copy()
        connector_type = config.pop("type", None) or config.pop("connector_type", None)
        name = config.pop("name", "unnamed")

        if not connector_type:
            raise ValueError("Configuration must include 'type' or 'connector_type'")

        return cls.create(connector_type, name, **config)

    @classmethod
    def _load_connector_class(cls, connector_type: ConnectorType) -> Optional[type]:
        """
        Lazy load a connector class.

        Args:
            connector_type: Type of connector.

        Returns:
            Connector class or None if not available.
        """
        if connector_type == ConnectorType.POSTGRESQL:
            from .postgresql import PostgreSQLConnector
            cls._connector_classes[connector_type] = PostgreSQLConnector
            return PostgreSQLConnector

        elif connector_type == ConnectorType.SNOWFLAKE:
            try:
                from .snowflake import SnowflakeConnector
                cls._connector_classes[connector_type] = SnowflakeConnector
                return SnowflakeConnector
            except ImportError:
                return None

        elif connector_type == ConnectorType.DATABRICKS:
            try:
                from .databricks import DatabricksConnector
                cls._connector_classes[connector_type] = DatabricksConnector
                return DatabricksConnector
            except ImportError:
                return None

        elif connector_type == ConnectorType.SQLSERVER:
            try:
                from .sqlserver import SQLServerConnector
                cls._connector_classes[connector_type] = SQLServerConnector
                return SQLServerConnector
            except ImportError:
                return None

        elif connector_type == ConnectorType.MYSQL:
            try:
                from .mysql import MySQLConnector
                cls._connector_classes[connector_type] = MySQLConnector
                return MySQLConnector
            except ImportError:
                return None

        elif connector_type == ConnectorType.ORACLE:
            try:
                from .oracle import OracleConnector
                cls._connector_classes[connector_type] = OracleConnector
                return OracleConnector
            except ImportError:
                return None

        return None

    @classmethod
    def _get_settings_for_type(cls, connector_type: ConnectorType, settings) -> dict[str, Any]:
        """
        Extract settings for a specific connector type.

        Args:
            connector_type: Connector type.
            settings: Application settings.

        Returns:
            Dictionary of connector parameters.
        """
        if connector_type == ConnectorType.POSTGRESQL:
            pg = settings.postgresql
            params = {
                "host": pg.host,
                "port": pg.port,
                "database": pg.database,
                "user": pg.user,
                "schema": "public",
            }
            if pg.password:
                params["password"] = pg.password.get_secret_value()
            return params

        elif connector_type == ConnectorType.SNOWFLAKE:
            sf = settings.snowflake
            params = {
                "account": sf.account,
                "user": sf.user,
                "warehouse": sf.warehouse,
                "database": sf.database,
                "schema": sf.schema_,
                "role": sf.role,
            }
            if sf.password:
                params["password"] = sf.password.get_secret_value()
            if sf.private_key_path:
                params["private_key_path"] = str(sf.private_key_path)
            if sf.private_key_passphrase:
                params["private_key_passphrase"] = sf.private_key_passphrase.get_secret_value()
            return params

        elif connector_type == ConnectorType.DATABRICKS:
            db = settings.databricks
            params = {
                "host": db.host,
                "http_path": db.http_path,
                "catalog": db.catalog,
                "schema": db.schema_,
            }
            if db.token:
                params["token"] = db.token.get_secret_value()
            return params

        elif connector_type == ConnectorType.SQLSERVER:
            ss = settings.sqlserver
            params = {
                "host": ss.host,
                "port": ss.port,
                "database": ss.database,
                "user": ss.user,
            }
            if ss.password:
                params["password"] = ss.password.get_secret_value()
            return params

        elif connector_type == ConnectorType.MYSQL:
            my = settings.mysql
            params = {
                "host": my.host,
                "port": my.port,
                "database": my.database,
                "user": my.user,
            }
            if my.password:
                params["password"] = my.password.get_secret_value()
            return params

        elif connector_type == ConnectorType.ORACLE:
            ora = settings.oracle
            params = {
                "host": ora.host,
                "port": ora.port,
                "service_name": ora.service_name,
                "user": ora.user,
            }
            if ora.password:
                params["password"] = ora.password.get_secret_value()
            return params

        return {}

    @classmethod
    def get_available_types(cls) -> list[ConnectorType]:
        """
        Get list of available connector types.

        Checks which connector dependencies are installed.

        Returns:
            List of available connector types.
        """
        available = []

        # PostgreSQL (psycopg2)
        try:
            import psycopg2
            available.append(ConnectorType.POSTGRESQL)
        except ImportError:
            pass

        # Snowflake
        try:
            import snowflake.connector
            available.append(ConnectorType.SNOWFLAKE)
        except ImportError:
            pass

        # Databricks
        try:
            import databricks.sql
            available.append(ConnectorType.DATABRICKS)
        except ImportError:
            pass

        # SQL Server (pymssql)
        try:
            import pymssql
            available.append(ConnectorType.SQLSERVER)
        except ImportError:
            pass

        # MySQL
        try:
            import pymysql
            available.append(ConnectorType.MYSQL)
        except ImportError:
            pass

        # Oracle
        try:
            import cx_Oracle
            available.append(ConnectorType.ORACLE)
        except ImportError:
            pass

        return available
