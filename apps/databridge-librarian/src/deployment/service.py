"""
Deployment Service - Orchestrates deployment workflows.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Callable

from sqlalchemy.orm import Session

from ..core.database import (
    get_session,
    Project,
    Hierarchy,
    Connection,
    DeploymentHistory,
)
from ..connections.base import AbstractDatabaseAdapter
from ..connections.adapters.snowflake import SnowflakeAdapter
from ..generation.ddl_generator import DDLGenerator, DDLConfig, SQLDialect
from .executor import DeploymentExecutor
from .models import (
    DeploymentStatus,
    DeploymentMode,
    DeploymentConfig,
    DeploymentPlan,
    DeploymentResult,
)

logger = logging.getLogger(__name__)


class DeploymentServiceError(Exception):
    """Base exception for deployment service errors."""
    pass


class ProjectNotFoundError(DeploymentServiceError):
    """Project not found."""
    pass


class ConnectionNotFoundError(DeploymentServiceError):
    """Connection not found."""
    pass


class DeploymentService:
    """
    Service for managing hierarchy deployments.

    Orchestrates:
    - DDL generation from hierarchies
    - Deployment execution to target databases
    - Deployment history tracking
    - Rollback operations
    """

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize deployment service.

        Args:
            session: Optional SQLAlchemy session
        """
        self._external_session = session is not None
        self._session = session

    def _get_session(self) -> Session:
        """Get or create database session."""
        if self._session is None:
            self._session = get_session()
        return self._session

    def _close_session(self) -> None:
        """Close session if internally created."""
        if not self._external_session and self._session:
            self._session.close()
            self._session = None

    def get_adapter_for_connection(
        self, connection: Connection
    ) -> AbstractDatabaseAdapter:
        """
        Create database adapter for a connection.

        Args:
            connection: Connection model

        Returns:
            Configured database adapter
        """
        if connection.connection_type.lower() == "snowflake":
            extra_config = connection.extra_config or {}
            return SnowflakeAdapter(
                host=connection.host,
                username=connection.username,
                password=connection.password_encrypted,  # Should be decrypted
                database=connection.database,
                extra_config={
                    "warehouse": extra_config.get("warehouse", "COMPUTE_WH"),
                    "role": extra_config.get("role"),
                    "schema": extra_config.get("schema", "PUBLIC"),
                },
            )
        else:
            raise DeploymentServiceError(
                f"Unsupported connection type: {connection.connection_type}"
            )

    def create_deployment_plan(
        self,
        project_id: str,
        connection_id: str,
        config: DeploymentConfig,
    ) -> DeploymentPlan:
        """
        Create a deployment plan for a project.

        Args:
            project_id: Project to deploy
            connection_id: Target database connection
            config: Deployment configuration

        Returns:
            DeploymentPlan ready for execution
        """
        session = self._get_session()

        try:
            # Load project
            project = session.query(Project).filter(Project.id == project_id).first()
            if not project:
                raise ProjectNotFoundError(f"Project not found: {project_id}")

            # Load hierarchies
            hierarchies = (
                session.query(Hierarchy)
                .filter(Hierarchy.project_id == project_id)
                .filter(Hierarchy.is_current == True)
                .all()
            )

            # Load connection
            connection = (
                session.query(Connection)
                .filter(Connection.id == connection_id)
                .first()
            )
            if not connection:
                raise ConnectionNotFoundError(f"Connection not found: {connection_id}")

            # Generate DDL scripts
            ddl_config = DDLConfig(
                dialect=self._get_dialect(connection.connection_type),
                target_database=config.target_database,
                target_schema=config.target_schema,
                include_drop=config.drop_existing,
                use_create_or_replace=True,
                generate_tbl_0=True,
                generate_vw_1=True,
                generate_dt_2=False,  # Dynamic tables optional
            )

            generator = DDLGenerator()
            generated_ddl = generator.generate(project, hierarchies, ddl_config)

            # Create adapter and executor
            adapter = self.get_adapter_for_connection(connection)
            executor = DeploymentExecutor(adapter)

            # Create plan
            plan = executor.create_plan(
                generated_ddl=generated_ddl,
                config=config,
                project_id=project_id,
                project_name=project.name,
            )

            return plan

        finally:
            if not self._external_session:
                self._close_session()

    def _get_dialect(self, connection_type: str) -> SQLDialect:
        """Map connection type to SQL dialect."""
        mapping = {
            "snowflake": SQLDialect.SNOWFLAKE,
            "postgresql": SQLDialect.POSTGRESQL,
            "postgres": SQLDialect.POSTGRESQL,
            "bigquery": SQLDialect.BIGQUERY,
            "sqlserver": SQLDialect.TSQL,
            "mysql": SQLDialect.MYSQL,
        }
        return mapping.get(connection_type.lower(), SQLDialect.SNOWFLAKE)

    def execute_deployment(
        self,
        project_id: str,
        connection_id: str,
        config: DeploymentConfig,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> DeploymentResult:
        """
        Execute a deployment.

        Args:
            project_id: Project to deploy
            connection_id: Target database connection
            config: Deployment configuration
            progress_callback: Optional progress callback

        Returns:
            DeploymentResult with execution details
        """
        session = self._get_session()

        try:
            # Load connection
            connection = (
                session.query(Connection)
                .filter(Connection.id == connection_id)
                .first()
            )
            if not connection:
                raise ConnectionNotFoundError(f"Connection not found: {connection_id}")

            # Create plan
            plan = self.create_deployment_plan(project_id, connection_id, config)

            if not plan.is_valid:
                result = DeploymentResult(
                    deployment_id="",
                    project_id=project_id,
                    status=DeploymentStatus.FAILED,
                    mode=config.mode,
                    target_database=config.target_database,
                    target_schema=config.target_schema,
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                    error_message="; ".join(plan.validation_errors),
                )
                return result

            # Create adapter and executor
            adapter = self.get_adapter_for_connection(connection)
            executor = DeploymentExecutor(adapter)

            if progress_callback:
                executor.set_progress_callback(progress_callback)

            # Execute
            result = executor.execute(plan, config)

            # Record deployment history
            self._record_deployment(
                session=session,
                project_id=project_id,
                connection_id=connection_id,
                result=result,
                config=config,
            )

            session.commit()
            return result

        except Exception as e:
            logger.exception("Deployment execution failed")
            session.rollback()
            raise DeploymentServiceError(f"Deployment failed: {e}") from e

        finally:
            if not self._external_session:
                self._close_session()

    def _record_deployment(
        self,
        session: Session,
        project_id: str,
        connection_id: str,
        result: DeploymentResult,
        config: DeploymentConfig,
    ) -> None:
        """Record deployment to history."""
        for script_result in result.script_results:
            history = DeploymentHistory(
                project_id=project_id,
                connection_id=connection_id,
                script_type=script_result.script.script_type.value,
                script_content=script_result.script.sql[:10000],  # Truncate
                target_database=config.target_database,
                target_schema=config.target_schema,
                target_table=script_result.script.object_name,
                status=script_result.status.value,
                error_message=script_result.error_message,
                rows_affected=script_result.rows_affected,
                executed_at=script_result.executed_at,
                executed_by=config.executed_by,
                duration_ms=script_result.execution_time_ms,
            )
            session.add(history)

    def get_deployment_history(
        self,
        project_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get deployment history for a project.

        Args:
            project_id: Project identifier
            limit: Maximum records to return

        Returns:
            List of deployment history records
        """
        session = self._get_session()

        try:
            history = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .order_by(DeploymentHistory.created_at.desc())
                .limit(limit)
                .all()
            )

            return [
                {
                    "id": h.id,
                    "project_id": h.project_id,
                    "connection_id": h.connection_id,
                    "script_type": h.script_type,
                    "target_database": h.target_database,
                    "target_schema": h.target_schema,
                    "target_table": h.target_table,
                    "status": h.status,
                    "error_message": h.error_message,
                    "rows_affected": h.rows_affected,
                    "executed_at": h.executed_at.isoformat() if h.executed_at else None,
                    "executed_by": h.executed_by,
                    "duration_ms": h.duration_ms,
                    "created_at": h.created_at.isoformat() if h.created_at else None,
                }
                for h in history
            ]

        finally:
            if not self._external_session:
                self._close_session()

    def get_deployment_summary(
        self,
        project_id: str,
    ) -> Dict[str, Any]:
        """
        Get deployment summary for a project.

        Args:
            project_id: Project identifier

        Returns:
            Summary with counts and latest deployment info
        """
        session = self._get_session()

        try:
            # Count total deployments
            total = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .count()
            )

            # Count by status
            successful = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .filter(DeploymentHistory.status == "success")
                .count()
            )

            failed = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .filter(DeploymentHistory.status == "failed")
                .count()
            )

            # Get latest deployment
            latest = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .order_by(DeploymentHistory.created_at.desc())
                .first()
            )

            return {
                "project_id": project_id,
                "total_deployments": total,
                "successful": successful,
                "failed": failed,
                "latest_deployment": {
                    "id": latest.id,
                    "status": latest.status,
                    "executed_at": latest.executed_at.isoformat() if latest and latest.executed_at else None,
                    "target": f"{latest.target_database}.{latest.target_schema}" if latest else None,
                } if latest else None,
            }

        finally:
            if not self._external_session:
                self._close_session()

    def rollback_deployment(
        self,
        project_id: str,
        connection_id: str,
        deployment_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Rollback a deployment by dropping created objects.

        Args:
            project_id: Project identifier
            connection_id: Connection identifier
            deployment_id: Optional specific deployment to rollback

        Returns:
            Rollback result
        """
        session = self._get_session()

        try:
            # Get deployment history to rollback
            query = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.project_id == project_id)
                .filter(DeploymentHistory.status == "success")
            )

            if deployment_id:
                # Rollback specific deployment
                history = query.filter(DeploymentHistory.id == deployment_id).all()
            else:
                # Rollback latest successful deployment
                history = query.order_by(DeploymentHistory.created_at.desc()).limit(100).all()

            if not history:
                return {
                    "success": False,
                    "message": "No successful deployments found to rollback",
                }

            # Load connection
            connection = (
                session.query(Connection)
                .filter(Connection.id == connection_id)
                .first()
            )
            if not connection:
                raise ConnectionNotFoundError(f"Connection not found: {connection_id}")

            # Create adapter
            adapter = self.get_adapter_for_connection(connection)
            adapter.connect()

            dropped = []
            errors = []

            try:
                # Get unique target info
                target_db = history[0].target_database
                target_schema = history[0].target_schema

                adapter.execute_query(f"USE DATABASE {target_db}")
                adapter.execute_query(f"USE SCHEMA {target_schema}")

                # Collect unique objects to drop (in reverse order)
                objects_to_drop = []
                seen = set()

                for h in reversed(history):
                    key = (h.script_type, h.target_table)
                    if key not in seen:
                        seen.add(key)
                        objects_to_drop.append(h)

                # Drop objects
                for h in objects_to_drop:
                    obj_type = self._get_object_type(h.script_type)
                    if obj_type:
                        drop_sql = f"DROP {obj_type} IF EXISTS {h.target_table}"
                        try:
                            adapter.execute_query(drop_sql)
                            dropped.append(h.target_table)
                        except Exception as e:
                            errors.append(f"{h.target_table}: {str(e)}")

            finally:
                adapter.disconnect()

            return {
                "success": len(errors) == 0,
                "dropped_objects": dropped,
                "errors": errors,
                "message": f"Rolled back {len(dropped)} objects",
            }

        finally:
            if not self._external_session:
                self._close_session()

    def _get_object_type(self, script_type: str) -> Optional[str]:
        """Get SQL object type from script type."""
        mapping = {
            "CREATE_TABLE": "TABLE",
            "CREATE_VIEW": "VIEW",
            "CREATE_DYNAMIC_TABLE": "DYNAMIC TABLE",
        }
        return mapping.get(script_type)

    def compare_deployment_versions(
        self,
        project_id: str,
        deployment_id_1: int,
        deployment_id_2: int,
    ) -> Dict[str, Any]:
        """
        Compare two deployment versions.

        Args:
            project_id: Project identifier
            deployment_id_1: First deployment ID
            deployment_id_2: Second deployment ID

        Returns:
            Comparison result
        """
        session = self._get_session()

        try:
            d1 = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.id == deployment_id_1)
                .filter(DeploymentHistory.project_id == project_id)
                .first()
            )

            d2 = (
                session.query(DeploymentHistory)
                .filter(DeploymentHistory.id == deployment_id_2)
                .filter(DeploymentHistory.project_id == project_id)
                .first()
            )

            if not d1 or not d2:
                return {
                    "error": "One or both deployments not found",
                }

            return {
                "deployment_1": {
                    "id": d1.id,
                    "executed_at": d1.executed_at.isoformat() if d1.executed_at else None,
                    "target": f"{d1.target_database}.{d1.target_schema}.{d1.target_table}",
                    "status": d1.status,
                    "script_type": d1.script_type,
                },
                "deployment_2": {
                    "id": d2.id,
                    "executed_at": d2.executed_at.isoformat() if d2.executed_at else None,
                    "target": f"{d2.target_database}.{d2.target_schema}.{d2.target_table}",
                    "status": d2.status,
                    "script_type": d2.script_type,
                },
                "differences": {
                    "same_target": (
                        d1.target_database == d2.target_database
                        and d1.target_schema == d2.target_schema
                        and d1.target_table == d2.target_table
                    ),
                    "same_script_type": d1.script_type == d2.script_type,
                    "script_content_changed": d1.script_content != d2.script_content,
                },
            }

        finally:
            if not self._external_session:
                self._close_session()
