"""
MCP Tools for Deployment Operations.

Provides tools for:
- Executing deployments to target databases
- Viewing deployment history
- Rolling back deployments
- Comparing deployment versions
"""

from typing import Dict, Any, Optional
from fastmcp import FastMCP


def register_deployment_tools(mcp: FastMCP) -> None:
    """Register all deployment MCP tools."""

    @mcp.tool()
    def execute_deployment(
        project_id: str,
        connection_id: str,
        target_database: str,
        target_schema: str,
        mode: str = "execute",
        stop_on_error: bool = True,
        drop_existing: bool = False,
        create_schema_if_not_exists: bool = True,
        executed_by: str = "mcp_user",
    ) -> Dict[str, Any]:
        """
        Execute a deployment of hierarchy DDL scripts to a target database.

        This tool generates DDL scripts from the project's hierarchies and
        executes them against the target database using the specified connection.

        Args:
            project_id: UUID of the project to deploy
            connection_id: UUID of the database connection to use
            target_database: Target database name (e.g., "ANALYTICS")
            target_schema: Target schema name (e.g., "HIERARCHIES")
            mode: Deployment mode - "dry_run" (validate only), "execute" (run scripts)
            stop_on_error: Stop deployment on first error (default: True)
            drop_existing: Drop existing objects before creating (default: False)
            create_schema_if_not_exists: Create target schema if missing (default: True)
            executed_by: User or system executing the deployment

        Returns:
            Dict with deployment result including:
            - deployment_id: Unique identifier for this deployment
            - status: "success", "failed", or "partial"
            - total_scripts: Number of scripts executed
            - successful_scripts: Count of successful scripts
            - failed_scripts: Count of failed scripts
            - total_execution_time_ms: Total execution time
            - error_message: Error details if failed
            - script_results: Details of each script execution

        Example:
            >>> execute_deployment(
            ...     project_id="abc123",
            ...     connection_id="conn456",
            ...     target_database="ANALYTICS",
            ...     target_schema="HIERARCHIES"
            ... )
        """
        from ...core.database import init_database
        from ...deployment import (
            DeploymentService,
            DeploymentConfig,
            DeploymentMode,
            DeploymentServiceError,
        )

        init_database()

        # Map mode string to enum
        mode_map = {
            "dry_run": DeploymentMode.DRY_RUN,
            "execute": DeploymentMode.EXECUTE,
            "execute_with_rollback": DeploymentMode.EXECUTE_WITH_ROLLBACK,
        }
        deployment_mode = mode_map.get(mode.lower(), DeploymentMode.EXECUTE)

        config = DeploymentConfig(
            target_database=target_database,
            target_schema=target_schema,
            mode=deployment_mode,
            stop_on_error=stop_on_error,
            drop_existing=drop_existing,
            create_schema_if_not_exists=create_schema_if_not_exists,
            executed_by=executed_by,
        )

        service = DeploymentService()

        try:
            result = service.execute_deployment(
                project_id=project_id,
                connection_id=connection_id,
                config=config,
            )
            return result.to_dict()
        except DeploymentServiceError as e:
            return {
                "error": str(e),
                "status": "failed",
            }

    @mcp.tool()
    def preview_deployment(
        project_id: str,
        connection_id: str,
        target_database: str,
        target_schema: str,
    ) -> Dict[str, Any]:
        """
        Preview a deployment without executing.

        Creates a deployment plan showing what scripts would be executed.
        Use this to validate before actual deployment.

        Args:
            project_id: UUID of the project to deploy
            connection_id: UUID of the database connection
            target_database: Target database name
            target_schema: Target schema name

        Returns:
            Dict with deployment plan including:
            - project_name: Name of the project
            - script_count: Number of scripts to execute
            - estimated_objects: Objects to be created
            - is_valid: Whether the plan is valid
            - validation_errors: List of validation issues
            - scripts: Details of each script (type, object name, tier)

        Example:
            >>> preview_deployment(
            ...     project_id="abc123",
            ...     connection_id="conn456",
            ...     target_database="ANALYTICS",
            ...     target_schema="HIERARCHIES"
            ... )
        """
        from ...core.database import init_database
        from ...deployment import (
            DeploymentService,
            DeploymentConfig,
            DeploymentMode,
            DeploymentServiceError,
        )

        init_database()

        config = DeploymentConfig(
            target_database=target_database,
            target_schema=target_schema,
            mode=DeploymentMode.DRY_RUN,
        )

        service = DeploymentService()

        try:
            plan = service.create_deployment_plan(
                project_id=project_id,
                connection_id=connection_id,
                config=config,
            )
            return plan.to_dict()
        except DeploymentServiceError as e:
            return {
                "error": str(e),
                "is_valid": False,
            }

    @mcp.tool()
    def get_deployment_history(
        project_id: str,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Get deployment history for a project.

        Returns recent deployments with their status and details.

        Args:
            project_id: UUID of the project
            limit: Maximum number of records to return (default: 50, max: 100)

        Returns:
            Dict with:
            - project_id: Project identifier
            - count: Number of records returned
            - deployments: List of deployment records with:
              - id: Deployment record ID
              - script_type: Type of script executed
              - target: Target database.schema.table
              - status: Execution status
              - executed_at: Execution timestamp
              - executed_by: User who executed
              - duration_ms: Execution time
              - error_message: Error if failed

        Example:
            >>> get_deployment_history(project_id="abc123", limit=10)
        """
        from ...core.database import init_database
        from ...deployment import DeploymentService

        init_database()

        limit = min(limit, 100)  # Cap at 100

        service = DeploymentService()
        history = service.get_deployment_history(project_id, limit)

        return {
            "project_id": project_id,
            "count": len(history),
            "deployments": history,
        }

    @mcp.tool()
    def get_deployment_summary(
        project_id: str,
    ) -> Dict[str, Any]:
        """
        Get deployment summary statistics for a project.

        Provides counts of total, successful, and failed deployments,
        plus information about the most recent deployment.

        Args:
            project_id: UUID of the project

        Returns:
            Dict with:
            - project_id: Project identifier
            - total_deployments: Total deployment count
            - successful: Count of successful deployments
            - failed: Count of failed deployments
            - latest_deployment: Details of most recent deployment

        Example:
            >>> get_deployment_summary(project_id="abc123")
        """
        from ...core.database import init_database
        from ...deployment import DeploymentService

        init_database()

        service = DeploymentService()
        return service.get_deployment_summary(project_id)

    @mcp.tool()
    def rollback_deployment(
        project_id: str,
        connection_id: str,
        deployment_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Rollback a deployment by dropping created objects.

        Drops objects created in the specified (or latest) deployment.
        Use with caution as this permanently removes database objects.

        Args:
            project_id: UUID of the project
            connection_id: UUID of the database connection
            deployment_id: Optional specific deployment ID to rollback.
                          If not specified, rolls back the latest deployment.

        Returns:
            Dict with:
            - success: Whether rollback completed successfully
            - dropped_objects: List of objects that were dropped
            - errors: List of any errors encountered
            - message: Summary message

        Example:
            >>> rollback_deployment(
            ...     project_id="abc123",
            ...     connection_id="conn456"
            ... )
        """
        from ...core.database import init_database
        from ...deployment import DeploymentService, DeploymentServiceError

        init_database()

        service = DeploymentService()

        try:
            return service.rollback_deployment(
                project_id=project_id,
                connection_id=connection_id,
                deployment_id=str(deployment_id) if deployment_id else None,
            )
        except DeploymentServiceError as e:
            return {
                "success": False,
                "error": str(e),
            }

    @mcp.tool()
    def compare_deployments(
        project_id: str,
        deployment_id_1: int,
        deployment_id_2: int,
    ) -> Dict[str, Any]:
        """
        Compare two deployment versions.

        Shows differences between two deployments including target,
        script type, and content changes.

        Args:
            project_id: UUID of the project
            deployment_id_1: First deployment ID
            deployment_id_2: Second deployment ID

        Returns:
            Dict with:
            - deployment_1: Details of first deployment
            - deployment_2: Details of second deployment
            - differences: Comparison results
              - same_target: Whether targets match
              - same_script_type: Whether script types match
              - script_content_changed: Whether content differs

        Example:
            >>> compare_deployments(
            ...     project_id="abc123",
            ...     deployment_id_1=1,
            ...     deployment_id_2=2
            ... )
        """
        from ...core.database import init_database
        from ...deployment import DeploymentService

        init_database()

        service = DeploymentService()
        return service.compare_deployment_versions(
            project_id=project_id,
            deployment_id_1=deployment_id_1,
            deployment_id_2=deployment_id_2,
        )
