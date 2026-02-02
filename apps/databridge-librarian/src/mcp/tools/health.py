"""
MCP Tools for Health and Readiness Checks.

Provides endpoints for service discovery and monitoring.
"""

from typing import Dict, Any, Optional


def register_health_tools(mcp) -> None:
    """Register health check MCP tools."""

    @mcp.tool()
    def get_service_health() -> Dict[str, Any]:
        """
        Get comprehensive service health status.

        Runs all registered health checks and returns a full report.

        Returns:
            JSON with overall status and individual check results.
        """
        from ..health import get_health_checker

        checker = get_health_checker()
        report = checker.readiness()
        return report.to_dict()

    @mcp.tool()
    def get_service_liveness() -> Dict[str, Any]:
        """
        Simple liveness check for the service.

        This is a lightweight check that returns immediately.
        Use this for Kubernetes liveness probes.

        Returns:
            JSON with liveness status.
        """
        from ..health import get_health_checker

        checker = get_health_checker()
        check = checker.liveness()
        return check.to_dict()

    @mcp.tool()
    def check_database_health(connection_id: str = "") -> Dict[str, Any]:
        """
        Check database connection health.

        Args:
            connection_id: Optional connection ID to check specific connection.
                          If empty, checks default database.

        Returns:
            JSON with database health status.
        """
        from ..health import get_health_checker, HealthCheck, HealthStatus

        checker = get_health_checker()

        # If connection_id provided, check specific connection
        if connection_id:
            try:
                from ..connections import get_connection_manager

                manager = get_connection_manager()
                connection = manager.get_connection(connection_id)

                if not connection:
                    return HealthCheck(
                        name=f"database:{connection_id}",
                        status=HealthStatus.UNHEALTHY,
                        message=f"Connection {connection_id} not found",
                    ).to_dict()

                # Test connection
                adapter = manager.get_adapter(connection_id)
                if adapter:
                    result = adapter.test_connection()
                    success = result[0] if isinstance(result, tuple) else result
                    return HealthCheck(
                        name=f"database:{connection_id}",
                        status=HealthStatus.HEALTHY if success else HealthStatus.UNHEALTHY,
                        message="Connection successful" if success else "Connection failed",
                    ).to_dict()
            except Exception as e:
                return HealthCheck(
                    name=f"database:{connection_id}",
                    status=HealthStatus.UNHEALTHY,
                    message=f"Error: {str(e)}",
                ).to_dict()

        # Default: check if storage is accessible
        try:
            from ..storage import init_database

            init_database()
            return HealthCheck(
                name="database:local",
                status=HealthStatus.HEALTHY,
                message="Local storage accessible",
            ).to_dict()
        except Exception as e:
            return HealthCheck(
                name="database:local",
                status=HealthStatus.UNHEALTHY,
                message=f"Storage error: {str(e)}",
            ).to_dict()

    @mcp.tool()
    def check_backend_health() -> Dict[str, Any]:
        """
        Check NestJS backend health.

        Tests connectivity to the backend API service.

        Returns:
            JSON with backend health status.
        """
        import os
        from ..health import get_health_checker

        checker = get_health_checker()
        backend_url = os.getenv("BACKEND_URL", "http://localhost:3002")

        check = checker.check_service(
            url=f"{backend_url}/api/health",
            name="backend",
            timeout=10.0,
        )
        return check.to_dict()

    @mcp.tool()
    def check_redis_health() -> Dict[str, Any]:
        """
        Check Redis connectivity.

        Returns:
            JSON with Redis health status.
        """
        import os
        from ..health import get_health_checker, HealthCheck, HealthStatus

        checker = get_health_checker()
        redis_url = os.getenv("REDIS_URL")

        if not redis_url:
            return HealthCheck(
                name="redis",
                status=HealthStatus.DEGRADED,
                message="Redis not configured (REDIS_URL not set)",
            ).to_dict()

        try:
            import redis

            client = redis.from_url(redis_url)
            check = checker.check_redis(client, name="redis")
            return check.to_dict()
        except ImportError:
            return HealthCheck(
                name="redis",
                status=HealthStatus.DEGRADED,
                message="Redis library not installed",
            ).to_dict()
        except Exception as e:
            return HealthCheck(
                name="redis",
                status=HealthStatus.UNHEALTHY,
                message=f"Redis error: {str(e)}",
            ).to_dict()

    @mcp.tool()
    def get_service_info() -> Dict[str, Any]:
        """
        Get service information and capabilities.

        Returns:
            JSON with service metadata including version, tools count,
            and available capabilities.
        """
        from ..health import get_health_checker

        checker = get_health_checker()

        return {
            "service": checker._service_name,
            "version": checker._version,
            "status": "running",
            "capabilities": {
                "hierarchy_management": True,
                "data_reconciliation": True,
                "source_discovery": True,
                "deployment": True,
                "templates": True,
                "skills": True,
            },
            "registered_health_checks": checker.get_registered_checks(),
        }
