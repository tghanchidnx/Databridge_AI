"""Unit tests for health checker."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from src.health.checker import (
    HealthStatus,
    HealthCheck,
    HealthReport,
    HealthChecker,
    get_health_checker,
    reset_health_checker,
)


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_health_status_values(self):
        """Test health status values exist."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.UNKNOWN.value == "unknown"


class TestHealthCheck:
    """Tests for HealthCheck dataclass."""

    def test_health_check_creation(self):
        """Test health check creation."""
        check = HealthCheck(
            name="database",
            status=HealthStatus.HEALTHY,
            message="Connected",
            latency_ms=15.5,
        )

        assert check.name == "database"
        assert check.status == HealthStatus.HEALTHY
        assert check.message == "Connected"
        assert check.latency_ms == 15.5

    def test_health_check_defaults(self):
        """Test health check default values."""
        check = HealthCheck(
            name="test",
            status=HealthStatus.HEALTHY,
        )

        assert check.message == ""
        assert check.latency_ms == 0.0
        assert check.details == {}
        assert check.timestamp is not None

    def test_health_check_to_dict(self):
        """Test health check serialization."""
        check = HealthCheck(
            name="database",
            status=HealthStatus.HEALTHY,
            message="Connected",
            latency_ms=15.5,
            details={"host": "localhost"},
        )
        data = check.to_dict()

        assert data["name"] == "database"
        assert data["status"] == "healthy"
        assert data["message"] == "Connected"
        assert data["latency_ms"] == 15.5
        assert data["details"]["host"] == "localhost"
        assert "timestamp" in data


class TestHealthReport:
    """Tests for HealthReport dataclass."""

    def test_health_report_creation(self):
        """Test health report creation."""
        checks = [
            HealthCheck(name="db", status=HealthStatus.HEALTHY),
            HealthCheck(name="redis", status=HealthStatus.HEALTHY),
        ]
        report = HealthReport(
            status=HealthStatus.HEALTHY,
            checks=checks,
        )

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 2

    def test_health_report_to_dict(self):
        """Test health report serialization."""
        checks = [
            HealthCheck(name="db", status=HealthStatus.HEALTHY),
        ]
        report = HealthReport(
            status=HealthStatus.HEALTHY,
            checks=checks,
            service="test-service",
            version="2.0.0",
        )
        data = report.to_dict()

        assert data["status"] == "healthy"
        assert data["service"] == "test-service"
        assert data["version"] == "2.0.0"
        assert len(data["checks"]) == 1
        assert "timestamp" in data


class TestHealthChecker:
    """Tests for HealthChecker class."""

    def setup_method(self):
        """Reset health checker before each test."""
        reset_health_checker()

    def teardown_method(self):
        """Clean up after each test."""
        reset_health_checker()

    def test_health_checker_initialization(self):
        """Test health checker initializes correctly."""
        checker = HealthChecker(
            service_name="test-service",
            version="1.0.0",
        )

        assert checker._service_name == "test-service"
        assert checker._version == "1.0.0"
        assert len(checker._checks) == 0

    def test_register_check(self):
        """Test registering a health check."""
        checker = HealthChecker()

        def check_fn():
            return HealthCheck(name="custom", status=HealthStatus.HEALTHY)

        checker.register_check("custom", check_fn)

        assert "custom" in checker._checks

    def test_unregister_check(self):
        """Test unregistering a health check."""
        checker = HealthChecker()

        def check_fn():
            return HealthCheck(name="custom", status=HealthStatus.HEALTHY)

        checker.register_check("custom", check_fn)
        result = checker.unregister_check("custom")

        assert result is True
        assert "custom" not in checker._checks

    def test_unregister_nonexistent_check(self):
        """Test unregistering check that doesn't exist."""
        checker = HealthChecker()
        result = checker.unregister_check("nonexistent")

        assert result is False

    def test_check_database_healthy(self):
        """Test database check when healthy."""
        checker = HealthChecker()

        check = checker.check_database(
            connection_fn=lambda: True,
            name="db",
        )

        assert check.status == HealthStatus.HEALTHY
        assert check.name == "db"
        assert check.latency_ms >= 0

    def test_check_database_unhealthy(self):
        """Test database check when unhealthy."""
        checker = HealthChecker()

        check = checker.check_database(
            connection_fn=lambda: False,
            name="db",
        )

        assert check.status == HealthStatus.UNHEALTHY

    def test_check_database_error(self):
        """Test database check when error occurs."""
        checker = HealthChecker()

        def failing_connection():
            raise ConnectionError("Connection refused")

        check = checker.check_database(
            connection_fn=failing_connection,
            name="db",
        )

        assert check.status == HealthStatus.UNHEALTHY
        assert "Connection refused" in check.message

    def test_check_redis_healthy(self):
        """Test Redis check when healthy."""
        checker = HealthChecker()

        mock_redis = MagicMock()
        mock_redis.ping.return_value = True

        check = checker.check_redis(mock_redis, name="redis")

        assert check.status == HealthStatus.HEALTHY
        mock_redis.ping.assert_called_once()

    def test_check_redis_not_configured(self):
        """Test Redis check when not configured."""
        checker = HealthChecker()

        check = checker.check_redis(None, name="redis")

        assert check.status == HealthStatus.DEGRADED
        assert "not configured" in check.message

    def test_check_redis_error(self):
        """Test Redis check when error occurs."""
        checker = HealthChecker()

        mock_redis = MagicMock()
        mock_redis.ping.side_effect = ConnectionError("Connection refused")

        check = checker.check_redis(mock_redis, name="redis")

        assert check.status == HealthStatus.UNHEALTHY
        assert "Connection refused" in check.message

    def test_check_service_without_httpx(self):
        """Test service check handles missing httpx gracefully."""
        checker = HealthChecker()

        # When httpx is not available or connection fails, should return unknown
        check = checker.check_service(
            url="http://nonexistent-host:9999/health",
            name="backend",
            timeout=1.0,  # Short timeout
        )

        # Should either be unhealthy (connection error) or unknown (httpx not installed)
        assert check.status in (HealthStatus.UNHEALTHY, HealthStatus.UNKNOWN)

    def test_check_service_connection_error(self):
        """Test service check handles connection errors."""
        checker = HealthChecker()

        check = checker.check_service(
            url="http://127.0.0.1:59999/health",  # Unlikely to be running
            name="backend",
            timeout=1.0,
        )

        # Should handle connection error gracefully
        assert check.status in (HealthStatus.UNHEALTHY, HealthStatus.UNKNOWN)
        assert check.name == "backend"

    def test_run_checks_all_healthy(self):
        """Test running all checks when healthy."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )
        checker.register_check(
            "check2",
            lambda: HealthCheck(name="check2", status=HealthStatus.HEALTHY),
        )

        report = checker.run_checks()

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) == 2

    def test_run_checks_one_unhealthy(self):
        """Test running checks when one is unhealthy."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )
        checker.register_check(
            "check2",
            lambda: HealthCheck(name="check2", status=HealthStatus.UNHEALTHY),
        )

        report = checker.run_checks()

        assert report.status == HealthStatus.UNHEALTHY

    def test_run_checks_one_degraded(self):
        """Test running checks when one is degraded."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )
        checker.register_check(
            "check2",
            lambda: HealthCheck(name="check2", status=HealthStatus.DEGRADED),
        )

        report = checker.run_checks()

        assert report.status == HealthStatus.DEGRADED

    def test_run_checks_filtered(self):
        """Test running specific checks only."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )
        checker.register_check(
            "check2",
            lambda: HealthCheck(name="check2", status=HealthStatus.HEALTHY),
        )

        report = checker.run_checks(names=["check1"])

        assert len(report.checks) == 1
        assert report.checks[0].name == "check1"

    def test_run_checks_with_error(self):
        """Test running checks when one raises error."""
        checker = HealthChecker()

        def failing_check():
            raise RuntimeError("Check failed")

        checker.register_check("failing", failing_check)

        report = checker.run_checks()

        assert report.status == HealthStatus.UNHEALTHY
        assert "Check failed" in report.checks[0].message

    def test_liveness_check(self):
        """Test liveness check always returns healthy."""
        checker = HealthChecker()

        check = checker.liveness()

        assert check.status == HealthStatus.HEALTHY
        assert check.name == "liveness"

    def test_readiness_check(self):
        """Test readiness runs all checks."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )

        report = checker.readiness()

        assert isinstance(report, HealthReport)
        assert len(report.checks) == 1

    def test_get_last_report(self):
        """Test getting last health report."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )

        checker.run_checks()
        report = checker.get_last_report()

        assert report is not None
        assert report.status == HealthStatus.HEALTHY

    def test_get_registered_checks(self):
        """Test getting registered check names."""
        checker = HealthChecker()

        checker.register_check(
            "check1",
            lambda: HealthCheck(name="check1", status=HealthStatus.HEALTHY),
        )
        checker.register_check(
            "check2",
            lambda: HealthCheck(name="check2", status=HealthStatus.HEALTHY),
        )

        names = checker.get_registered_checks()

        assert "check1" in names
        assert "check2" in names

    def test_compute_overall_status_empty(self):
        """Test computing status with no checks."""
        checker = HealthChecker()

        status = checker._compute_overall_status([])

        assert status == HealthStatus.UNKNOWN


class TestGlobalHealthChecker:
    """Tests for global health checker functions."""

    def setup_method(self):
        """Reset health checker before each test."""
        reset_health_checker()

    def teardown_method(self):
        """Clean up after each test."""
        reset_health_checker()

    def test_get_health_checker_creates_instance(self):
        """Test get_health_checker creates singleton."""
        checker = get_health_checker()

        assert checker is not None
        assert isinstance(checker, HealthChecker)

    def test_get_health_checker_returns_same_instance(self):
        """Test get_health_checker returns same instance."""
        checker1 = get_health_checker()
        checker2 = get_health_checker()

        assert checker1 is checker2

    def test_reset_health_checker(self):
        """Test reset_health_checker creates new instance."""
        checker1 = get_health_checker()
        reset_health_checker()
        checker2 = get_health_checker()

        assert checker1 is not checker2

    def test_get_health_checker_with_params(self):
        """Test get_health_checker with custom params."""
        checker = get_health_checker(
            service_name="custom-service",
            version="2.0.0",
        )

        assert checker._service_name == "custom-service"
        assert checker._version == "2.0.0"
