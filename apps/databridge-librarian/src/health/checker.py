"""
Health Checker for DataBridge Librarian.

Provides health and readiness checks for service discovery.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List, Callable

logger = logging.getLogger(__name__)


class HealthStatus(str, Enum):
    """Health check status values."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Result of a single health check."""

    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class HealthReport:
    """Complete health report from all checks."""

    status: HealthStatus
    checks: List[HealthCheck]
    version: str = "1.0.0"
    service: str = "databridge-librarian"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.value,
            "service": self.service,
            "version": self.version,
            "checks": [c.to_dict() for c in self.checks],
            "timestamp": self.timestamp.isoformat(),
        }


class HealthChecker:
    """
    Health checker for service health and readiness.

    Supports:
    - Database connectivity checks
    - Redis connectivity checks
    - Custom health checks
    - Readiness vs liveness distinction
    """

    def __init__(
        self,
        service_name: str = "databridge-librarian",
        version: str = "1.0.0",
    ):
        """
        Initialize health checker.

        Args:
            service_name: Name of the service
            version: Service version
        """
        self._service_name = service_name
        self._version = version
        self._checks: Dict[str, Callable[[], HealthCheck]] = {}
        self._last_report: Optional[HealthReport] = None

    def register_check(
        self,
        name: str,
        check_fn: Callable[[], HealthCheck],
    ) -> None:
        """
        Register a health check function.

        Args:
            name: Name of the check
            check_fn: Function that returns HealthCheck
        """
        self._checks[name] = check_fn
        logger.info(f"Registered health check: {name}")

    def unregister_check(self, name: str) -> bool:
        """
        Unregister a health check.

        Args:
            name: Name of the check

        Returns:
            True if check was removed
        """
        if name in self._checks:
            del self._checks[name]
            logger.info(f"Unregistered health check: {name}")
            return True
        return False

    def check_database(
        self,
        connection_fn: Callable[[], bool],
        name: str = "database",
    ) -> HealthCheck:
        """
        Check database connectivity.

        Args:
            connection_fn: Function that returns True if connected
            name: Name for the check

        Returns:
            HealthCheck result
        """
        import time

        start = time.time()
        try:
            connected = connection_fn()
            latency = (time.time() - start) * 1000

            if connected:
                return HealthCheck(
                    name=name,
                    status=HealthStatus.HEALTHY,
                    message="Database connection successful",
                    latency_ms=latency,
                )
            else:
                return HealthCheck(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message="Database connection failed",
                    latency_ms=latency,
                )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheck(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database error: {str(e)}",
                latency_ms=latency,
            )

    def check_redis(
        self,
        redis_client: Any,
        name: str = "redis",
    ) -> HealthCheck:
        """
        Check Redis connectivity.

        Args:
            redis_client: Redis client instance
            name: Name for the check

        Returns:
            HealthCheck result
        """
        import time

        start = time.time()
        try:
            if redis_client is None:
                return HealthCheck(
                    name=name,
                    status=HealthStatus.DEGRADED,
                    message="Redis not configured",
                    latency_ms=0,
                )

            # Ping Redis
            redis_client.ping()
            latency = (time.time() - start) * 1000

            return HealthCheck(
                name=name,
                status=HealthStatus.HEALTHY,
                message="Redis connection successful",
                latency_ms=latency,
            )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheck(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Redis error: {str(e)}",
                latency_ms=latency,
            )

    def check_service(
        self,
        url: str,
        timeout: float = 5.0,
        name: str = "service",
    ) -> HealthCheck:
        """
        Check external service health.

        Args:
            url: Health endpoint URL
            timeout: Request timeout
            name: Name for the check

        Returns:
            HealthCheck result
        """
        import time

        start = time.time()
        try:
            import httpx

            with httpx.Client(timeout=timeout) as client:
                response = client.get(url)
                latency = (time.time() - start) * 1000

                if response.status_code == 200:
                    return HealthCheck(
                        name=name,
                        status=HealthStatus.HEALTHY,
                        message=f"Service healthy at {url}",
                        latency_ms=latency,
                        details={"status_code": response.status_code},
                    )
                else:
                    return HealthCheck(
                        name=name,
                        status=HealthStatus.UNHEALTHY,
                        message=f"Service returned {response.status_code}",
                        latency_ms=latency,
                        details={"status_code": response.status_code},
                    )
        except ImportError:
            return HealthCheck(
                name=name,
                status=HealthStatus.UNKNOWN,
                message="httpx not available",
            )
        except Exception as e:
            latency = (time.time() - start) * 1000
            return HealthCheck(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Service error: {str(e)}",
                latency_ms=latency,
            )

    def run_checks(self, names: Optional[List[str]] = None) -> HealthReport:
        """
        Run health checks.

        Args:
            names: Optional list of check names to run (all if None)

        Returns:
            HealthReport with all results
        """
        checks_to_run = names or list(self._checks.keys())
        results: List[HealthCheck] = []

        for name in checks_to_run:
            if name in self._checks:
                try:
                    result = self._checks[name]()
                    results.append(result)
                except Exception as e:
                    logger.exception(f"Health check {name} failed")
                    results.append(
                        HealthCheck(
                            name=name,
                            status=HealthStatus.UNHEALTHY,
                            message=f"Check error: {str(e)}",
                        )
                    )

        # Determine overall status
        overall_status = self._compute_overall_status(results)

        report = HealthReport(
            status=overall_status,
            checks=results,
            version=self._version,
            service=self._service_name,
        )
        self._last_report = report
        return report

    def _compute_overall_status(self, checks: List[HealthCheck]) -> HealthStatus:
        """Compute overall status from individual checks."""
        if not checks:
            return HealthStatus.UNKNOWN

        statuses = [c.status for c in checks]

        if all(s == HealthStatus.HEALTHY for s in statuses):
            return HealthStatus.HEALTHY
        elif any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.UNKNOWN

    def liveness(self) -> HealthCheck:
        """
        Simple liveness check (is service running?).

        Returns:
            HealthCheck for liveness
        """
        return HealthCheck(
            name="liveness",
            status=HealthStatus.HEALTHY,
            message="Service is alive",
        )

    def readiness(self) -> HealthReport:
        """
        Full readiness check (is service ready to accept traffic?).

        Returns:
            HealthReport with all checks
        """
        return self.run_checks()

    def get_last_report(self) -> Optional[HealthReport]:
        """Get the last health report."""
        return self._last_report

    def get_registered_checks(self) -> List[str]:
        """Get list of registered check names."""
        return list(self._checks.keys())


# Global health checker instance
_health_checker: Optional[HealthChecker] = None


def get_health_checker(
    service_name: str = "databridge-librarian",
    version: str = "1.0.0",
) -> HealthChecker:
    """
    Get the global health checker instance.

    Args:
        service_name: Service name
        version: Service version

    Returns:
        HealthChecker instance
    """
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker(
            service_name=service_name,
            version=version,
        )
    return _health_checker


def reset_health_checker() -> None:
    """Reset the global health checker (for testing)."""
    global _health_checker
    _health_checker = None
