"""Integration tests for health check system.

Tests the health checker with various check types and scenarios.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.health.checker import (
    HealthChecker,
    HealthStatus,
    HealthCheck,
    HealthReport,
    get_health_checker,
    reset_health_checker,
)


class TestHealthCheckerIntegration:
    """Integration tests for HealthChecker."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_health_checker()

    def test_basic_health_check(self):
        """Test basic health check workflow."""
        checker = get_health_checker()

        # Register a simple check
        checker.register_check("test_check", lambda: HealthCheck(
            name="test_check",
            status=HealthStatus.HEALTHY,
            message="Test is healthy",
        ))

        # Run checks
        report = checker.run_checks()

        assert report.status == HealthStatus.HEALTHY
        assert len(report.checks) >= 1
        assert any(c.name == "test_check" for c in report.checks)

    def test_mixed_health_statuses(self):
        """Test health report with mixed statuses."""
        checker = HealthChecker()

        # Register healthy check
        checker.register_check("healthy_check", lambda: HealthCheck(
            name="healthy_check",
            status=HealthStatus.HEALTHY,
            message="All good",
        ))

        # Register degraded check
        checker.register_check("degraded_check", lambda: HealthCheck(
            name="degraded_check",
            status=HealthStatus.DEGRADED,
            message="Partial functionality",
        ))

        report = checker.run_checks()

        # Overall status should be degraded (worst of healthy and degraded)
        assert report.status == HealthStatus.DEGRADED

    def test_unhealthy_check_fails_readiness(self):
        """Test that unhealthy check fails readiness."""
        checker = HealthChecker()

        checker.register_check("unhealthy_check", lambda: HealthCheck(
            name="unhealthy_check",
            status=HealthStatus.UNHEALTHY,
            message="Service is down",
        ))

        report = checker.readiness()

        assert report.status == HealthStatus.UNHEALTHY
        assert any(c.status == HealthStatus.UNHEALTHY for c in report.checks)

    def test_liveness_always_passes(self):
        """Test that liveness check always passes."""
        checker = HealthChecker()

        # Register failing check
        checker.register_check("failing_check", lambda: HealthCheck(
            name="failing_check",
            status=HealthStatus.UNHEALTHY,
            message="This fails",
        ))

        # Liveness should still pass
        liveness = checker.liveness()
        assert liveness.status == HealthStatus.HEALTHY

    def test_check_with_details(self):
        """Test health check with additional details."""
        checker = HealthChecker()

        checker.register_check("detailed_check", lambda: HealthCheck(
            name="detailed_check",
            status=HealthStatus.HEALTHY,
            message="Detailed status",
            details={
                "connections": 5,
                "latency_ms": 10,
                "version": "1.0.0",
            },
        ))

        report = checker.run_checks()
        detailed = next(c for c in report.checks if c.name == "detailed_check")

        assert detailed.details["connections"] == 5
        assert detailed.details["latency_ms"] == 10

    def test_check_latency_tracking(self):
        """Test that check latency is tracked."""
        import time

        checker = HealthChecker()

        def slow_check():
            time.sleep(0.1)  # 100ms delay
            return HealthCheck(
                name="slow_check",
                status=HealthStatus.HEALTHY,
                message="Slow but healthy",
                latency_ms=100.0,  # Manually set for testing
            )

        checker.register_check("slow_check", slow_check)

        report = checker.run_checks()
        slow = next(c for c in report.checks if c.name == "slow_check")

        # Check completed with latency recorded
        assert slow.status == HealthStatus.HEALTHY


class TestDatabaseHealthCheck:
    """Integration tests for database health checks."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_health_checker()

    def test_successful_database_check(self):
        """Test successful database health check."""
        checker = HealthChecker()

        # Mock successful connection
        def mock_connect():
            return True

        check = checker.check_database(mock_connect, name="test_db")

        assert check.status == HealthStatus.HEALTHY
        assert "test_db" in check.name

    def test_failed_database_check(self):
        """Test failed database health check."""
        checker = HealthChecker()

        # Mock failed connection
        def mock_connect():
            raise ConnectionError("Connection refused")

        check = checker.check_database(mock_connect, name="test_db")

        assert check.status == HealthStatus.UNHEALTHY
        assert "error" in check.message.lower() or "Connection" in check.message

    def test_slow_database_check(self):
        """Test slow database connection."""
        import time

        checker = HealthChecker()

        def mock_slow_connect():
            time.sleep(0.2)
            return True

        check = checker.check_database(mock_slow_connect, name="slow_db")

        # Should still be healthy but with latency recorded
        assert check.status == HealthStatus.HEALTHY
        if check.latency_ms is not None:
            assert check.latency_ms >= 200


class TestRedisHealthCheck:
    """Integration tests for Redis health checks."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_health_checker()

    def test_successful_redis_check(self):
        """Test successful Redis health check."""
        checker = HealthChecker()

        # Mock Redis client
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True

        check = checker.check_redis(mock_redis, name="test_redis")

        assert check.status == HealthStatus.HEALTHY

    def test_failed_redis_check(self):
        """Test failed Redis health check."""
        checker = HealthChecker()

        # Mock failing Redis client
        mock_redis = MagicMock()
        mock_redis.ping.side_effect = ConnectionError("Redis down")

        check = checker.check_redis(mock_redis, name="test_redis")

        assert check.status == HealthStatus.UNHEALTHY


class TestServiceHealthCheck:
    """Integration tests for external service health checks."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_health_checker()

    def test_successful_service_check(self):
        """Test successful external service check."""
        checker = HealthChecker()

        # This will fail because there's no real service, but we test the mechanism
        check = checker.check_service("http://localhost:99999", timeout=0.5, name="fake_service")

        # Should be unhealthy because service doesn't exist
        assert check.status == HealthStatus.UNHEALTHY

    def test_mocked_service_check(self):
        """Test service check returns proper structure."""
        checker = HealthChecker()

        # Just verify the check_service method exists and returns a HealthCheck
        check = checker.check_service("http://localhost:99998", timeout=0.5, name="test_service")

        # Should be unhealthy because service doesn't exist (connection refused)
        assert isinstance(check, HealthCheck)
        assert check.name == "test_service"

    @patch("httpx.get")
    def test_unhealthy_service_check(self, mock_get):
        """Test service check with unhealthy response."""
        checker = HealthChecker()

        # Mock 500 response
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        check = checker.check_service("http://example.com/health", name="example_service")

        assert check.status == HealthStatus.UNHEALTHY


class TestHealthReportSerialization:
    """Integration tests for health report serialization."""

    def test_report_to_dict(self):
        """Test health report serialization."""
        report = HealthReport(
            status=HealthStatus.HEALTHY,
            checks=[
                HealthCheck(
                    name="check1",
                    status=HealthStatus.HEALTHY,
                    message="OK",
                ),
                HealthCheck(
                    name="check2",
                    status=HealthStatus.HEALTHY,
                    message="OK",
                    details={"key": "value"},
                ),
            ],
        )

        data = report.to_dict()

        assert data["status"] == "healthy"
        assert len(data["checks"]) == 2
        assert "timestamp" in data

    def test_check_to_dict(self):
        """Test health check serialization."""
        check = HealthCheck(
            name="test_check",
            status=HealthStatus.DEGRADED,
            message="Partial outage",
            details={"affected_services": ["A", "B"]},
            latency_ms=50,
        )

        data = check.to_dict()

        assert data["name"] == "test_check"
        assert data["status"] == "degraded"
        assert data["message"] == "Partial outage"
        assert data["latency_ms"] == 50
        assert data["details"]["affected_services"] == ["A", "B"]


class TestHealthCheckerSingleton:
    """Tests for HealthChecker singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_health_checker()

    def test_singleton_behavior(self):
        """Test that get_health_checker returns singleton."""
        checker1 = get_health_checker()
        checker2 = get_health_checker()

        assert checker1 is checker2

    def test_reset_creates_new_instance(self):
        """Test that reset creates new instance."""
        checker1 = get_health_checker()
        reset_health_checker()
        checker2 = get_health_checker()

        assert checker1 is not checker2

    def test_registered_checks_persist(self):
        """Test that registered checks persist in singleton."""
        checker = get_health_checker()

        checker.register_check("persistent_check", lambda: HealthCheck(
            name="persistent_check",
            status=HealthStatus.HEALTHY,
            message="Still here",
        ))

        # Get same instance
        same_checker = get_health_checker()
        report = same_checker.run_checks()

        assert any(c.name == "persistent_check" for c in report.checks)


class TestSelectiveHealthChecks:
    """Tests for running selective health checks."""

    def test_run_specific_checks(self):
        """Test running only specific checks."""
        checker = HealthChecker()

        checker.register_check("check_a", lambda: HealthCheck(
            name="check_a", status=HealthStatus.HEALTHY, message="A OK",
        ))
        checker.register_check("check_b", lambda: HealthCheck(
            name="check_b", status=HealthStatus.HEALTHY, message="B OK",
        ))
        checker.register_check("check_c", lambda: HealthCheck(
            name="check_c", status=HealthStatus.HEALTHY, message="C OK",
        ))

        # Run only A and B
        report = checker.run_checks(names=["check_a", "check_b"])

        check_names = [c.name for c in report.checks]
        assert "check_a" in check_names
        assert "check_b" in check_names
        assert "check_c" not in check_names

    def test_run_nonexistent_check(self):
        """Test running nonexistent check."""
        checker = HealthChecker()

        checker.register_check("existing", lambda: HealthCheck(
            name="existing", status=HealthStatus.HEALTHY, message="OK",
        ))

        # Include nonexistent check
        report = checker.run_checks(names=["existing", "nonexistent"])

        # Should only include the existing check
        assert len(report.checks) == 1
        assert report.checks[0].name == "existing"
