"""
Deployment Executor - Executes DDL scripts against target databases.
"""

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Callable, Any

from ..connections.base import AbstractDatabaseAdapter
from ..generation.ddl_generator import GeneratedDDL, DDLType
from .models import (
    DeploymentStatus,
    DeploymentMode,
    ScriptType,
    DeploymentScript,
    ScriptExecutionResult,
    DeploymentPlan,
    DeploymentResult,
    DeploymentConfig,
)

logger = logging.getLogger(__name__)


class DeploymentError(Exception):
    """Base exception for deployment errors."""
    pass


class ValidationError(DeploymentError):
    """Validation error during deployment."""
    pass


class ExecutionError(DeploymentError):
    """Execution error during deployment."""
    pass


class DeploymentExecutor:
    """
    Executes deployment scripts against target databases.

    Handles:
    - Script execution with progress tracking
    - Transaction management and rollback
    - Error handling and recovery
    - Deployment history recording
    """

    def __init__(self, adapter: AbstractDatabaseAdapter):
        """
        Initialize executor with a database adapter.

        Args:
            adapter: Database adapter for executing SQL
        """
        self.adapter = adapter
        self._progress_callback: Optional[Callable[[str, int, int], None]] = None

    def set_progress_callback(
        self, callback: Callable[[str, int, int], None]
    ) -> None:
        """
        Set callback for progress updates.

        Args:
            callback: Function(message, current, total)
        """
        self._progress_callback = callback

    def _report_progress(self, message: str, current: int, total: int) -> None:
        """Report progress if callback is set."""
        if self._progress_callback:
            self._progress_callback(message, current, total)
        logger.info(f"[{current}/{total}] {message}")

    def create_plan(
        self,
        generated_ddl: List[GeneratedDDL],
        config: DeploymentConfig,
        project_id: str,
        project_name: str,
    ) -> DeploymentPlan:
        """
        Create a deployment plan from generated DDL.

        Args:
            generated_ddl: List of generated DDL scripts
            config: Deployment configuration
            project_id: Project identifier
            project_name: Project name

        Returns:
            DeploymentPlan ready for execution
        """
        plan = DeploymentPlan(
            project_id=project_id,
            project_name=project_name,
            target_database=config.target_database,
            target_schema=config.target_schema,
        )

        # Convert GeneratedDDL to DeploymentScript
        for ddl in generated_ddl:
            script_type = self._map_ddl_type(ddl.ddl_type)
            script = DeploymentScript(
                script_type=script_type,
                object_name=ddl.object_name,
                sql=ddl.sql,
                tier=ddl.tier,
                schema_name=ddl.schema_name or config.target_schema,
                database_name=config.target_database,
                dependencies=ddl.dependencies or [],
            )
            plan.scripts.append(script)

        # Determine execution order (respecting dependencies)
        plan.execution_order = self._resolve_execution_order(plan.scripts)
        plan.estimated_objects = len(plan.scripts)

        # Validate the plan
        plan.validation_errors = self._validate_plan(plan, config)

        return plan

    def _map_ddl_type(self, ddl_type: DDLType) -> ScriptType:
        """Map DDLType to ScriptType."""
        mapping = {
            DDLType.CREATE_TABLE: ScriptType.CREATE_TABLE,
            DDLType.CREATE_VIEW: ScriptType.CREATE_VIEW,
            DDLType.CREATE_DYNAMIC_TABLE: ScriptType.CREATE_DYNAMIC_TABLE,
            DDLType.INSERT: ScriptType.INSERT,
            DDLType.MERGE: ScriptType.MERGE,
            DDLType.DROP: ScriptType.DROP,
            DDLType.ALTER_TABLE: ScriptType.ALTER,
            DDLType.GRANT: ScriptType.GRANT,
        }
        return mapping.get(ddl_type, ScriptType.CREATE_TABLE)

    def _resolve_execution_order(self, scripts: List[DeploymentScript]) -> List[int]:
        """
        Resolve execution order based on dependencies.

        Uses topological sort to ensure dependencies are executed first.
        """
        # Build dependency graph
        name_to_idx = {s.object_name: i for i, s in enumerate(scripts)}
        order = []
        visited = set()
        temp_visited = set()

        def visit(idx: int) -> None:
            if idx in temp_visited:
                raise ValidationError(f"Circular dependency detected for {scripts[idx].object_name}")
            if idx in visited:
                return

            temp_visited.add(idx)
            script = scripts[idx]

            for dep in script.dependencies:
                if dep in name_to_idx:
                    visit(name_to_idx[dep])

            temp_visited.remove(idx)
            visited.add(idx)
            order.append(idx)

        for i in range(len(scripts)):
            if i not in visited:
                visit(i)

        return order

    def _validate_plan(
        self, plan: DeploymentPlan, config: DeploymentConfig
    ) -> List[str]:
        """Validate deployment plan."""
        errors = []

        if not plan.scripts:
            errors.append("No scripts to deploy")

        if not config.target_database:
            errors.append("Target database not specified")

        if not config.target_schema:
            errors.append("Target schema not specified")

        # Check for SQL injection patterns (basic validation)
        for script in plan.scripts:
            if self._contains_dangerous_patterns(script.sql):
                errors.append(
                    f"Script {script.object_name} contains potentially dangerous SQL patterns"
                )

        return errors

    def _contains_dangerous_patterns(self, sql: str) -> bool:
        """Check for dangerous SQL patterns."""
        dangerous_patterns = [
            r";\s*DROP\s+DATABASE",
            r";\s*DROP\s+SCHEMA.*CASCADE",
            r"TRUNCATE\s+.*\s*;\s*DROP",
            r"--.*DROP",
        ]
        sql_upper = sql.upper()
        for pattern in dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                return True
        return False

    def execute(
        self,
        plan: DeploymentPlan,
        config: DeploymentConfig,
    ) -> DeploymentResult:
        """
        Execute a deployment plan.

        Args:
            plan: Deployment plan to execute
            config: Deployment configuration

        Returns:
            DeploymentResult with execution details
        """
        deployment_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)

        result = DeploymentResult(
            deployment_id=deployment_id,
            project_id=plan.project_id,
            status=DeploymentStatus.PENDING,
            mode=config.mode,
            target_database=config.target_database,
            target_schema=config.target_schema,
            started_at=started_at,
        )

        # Dry run - just validate
        if config.mode == DeploymentMode.DRY_RUN:
            result.status = DeploymentStatus.SUCCESS if plan.is_valid else DeploymentStatus.FAILED
            if not plan.is_valid:
                result.error_message = "; ".join(plan.validation_errors)
            result.completed_at = datetime.now(timezone.utc)
            return result

        # Validate before execution
        if not plan.is_valid:
            result.status = DeploymentStatus.FAILED
            result.error_message = "; ".join(plan.validation_errors)
            result.completed_at = datetime.now(timezone.utc)
            return result

        try:
            # Connect to database
            self.adapter.connect()
            result.status = DeploymentStatus.EXECUTING

            # Set database context
            self._setup_context(config)

            # Execute scripts in order
            total_scripts = len(plan.scripts)
            for idx, script_idx in enumerate(plan.execution_order):
                script = plan.scripts[script_idx]
                self._report_progress(
                    f"Executing {script.script_type.value}: {script.object_name}",
                    idx + 1,
                    total_scripts,
                )

                script_result = self._execute_script(script, config)
                result.script_results.append(script_result)

                if not script_result.success:
                    if config.stop_on_error:
                        result.status = DeploymentStatus.FAILED
                        result.error_message = (
                            f"Failed at {script.object_name}: {script_result.error_message}"
                        )
                        break
                    else:
                        result.status = DeploymentStatus.PARTIAL

            # Finalize result
            if result.status == DeploymentStatus.EXECUTING:
                result.status = DeploymentStatus.SUCCESS

            result.completed_at = datetime.now(timezone.utc)

        except Exception as e:
            logger.exception("Deployment failed")
            result.status = DeploymentStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.now(timezone.utc)

        finally:
            try:
                self.adapter.disconnect()
            except Exception:
                pass

        return result

    def _setup_context(self, config: DeploymentConfig) -> None:
        """Set up database context for deployment."""
        # Use database
        self.adapter.execute_query(f"USE DATABASE {config.target_database}")

        # Create schema if needed
        if config.create_schema_if_not_exists:
            self.adapter.execute_query(
                f"CREATE SCHEMA IF NOT EXISTS {config.target_schema}"
            )

        # Use schema
        self.adapter.execute_query(f"USE SCHEMA {config.target_schema}")

    def _execute_script(
        self,
        script: DeploymentScript,
        config: DeploymentConfig,
    ) -> ScriptExecutionResult:
        """Execute a single script."""
        executed_at = datetime.now(timezone.utc)

        try:
            # Handle DROP if needed
            if config.drop_existing and script.script_type in (
                ScriptType.CREATE_TABLE,
                ScriptType.CREATE_VIEW,
                ScriptType.CREATE_DYNAMIC_TABLE,
            ):
                drop_sql = self._generate_drop_sql(script)
                if drop_sql:
                    self.adapter.execute_query(drop_sql)

            # Execute the main script
            query_result = self.adapter.execute_query(script.sql)

            return ScriptExecutionResult(
                script=script,
                status=DeploymentStatus.SUCCESS,
                rows_affected=query_result.row_count,
                execution_time_ms=query_result.execution_time_ms,
                executed_at=executed_at,
            )

        except Exception as e:
            logger.error(f"Script execution failed: {script.object_name} - {e}")
            return ScriptExecutionResult(
                script=script,
                status=DeploymentStatus.FAILED,
                error_message=str(e),
                executed_at=executed_at,
            )

    def _generate_drop_sql(self, script: DeploymentScript) -> Optional[str]:
        """Generate DROP statement for a script."""
        type_map = {
            ScriptType.CREATE_TABLE: "TABLE",
            ScriptType.CREATE_VIEW: "VIEW",
            ScriptType.CREATE_DYNAMIC_TABLE: "DYNAMIC TABLE",
        }
        obj_type = type_map.get(script.script_type)
        if obj_type:
            return f"DROP {obj_type} IF EXISTS {script.full_name}"
        return None

    def rollback(
        self,
        result: DeploymentResult,
        config: DeploymentConfig,
    ) -> DeploymentResult:
        """
        Rollback a deployment by dropping created objects.

        Args:
            result: Result of the deployment to rollback
            config: Deployment configuration

        Returns:
            Updated DeploymentResult
        """
        if result.status not in (
            DeploymentStatus.SUCCESS,
            DeploymentStatus.PARTIAL,
            DeploymentStatus.FAILED,
        ):
            raise DeploymentError("Cannot rollback deployment in current state")

        try:
            self.adapter.connect()
            self._setup_context(config)

            # Drop objects in reverse order
            successful_scripts = [
                r for r in result.script_results if r.success
            ]

            for script_result in reversed(successful_scripts):
                script = script_result.script
                drop_sql = self._generate_drop_sql(script)
                if drop_sql:
                    try:
                        self.adapter.execute_query(drop_sql)
                        logger.info(f"Rolled back: {script.object_name}")
                    except Exception as e:
                        logger.warning(
                            f"Could not rollback {script.object_name}: {e}"
                        )

            result.rollback_executed = True
            result.status = DeploymentStatus.ROLLED_BACK

        except Exception as e:
            logger.exception("Rollback failed")
            result.error_message = f"Rollback failed: {e}"

        finally:
            try:
                self.adapter.disconnect()
            except Exception:
                pass

        return result

    def validate_connection(self) -> bool:
        """
        Validate database connection.

        Returns:
            True if connection is valid
        """
        try:
            result = self.adapter.test_connection()
            # test_connection returns (bool, str) tuple
            if isinstance(result, tuple):
                return result[0]
            return bool(result)
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
