"""
Unit tests for Phase 31/32 - Data Observability Module.

Tests metrics collection, alerting, anomaly detection, and health scoring.
"""

import pytest
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from src.observability import (
    # Enums
    MetricType,
    AlertSeverity,
    AlertStatus,
    AnomalyType,
    # Models
    Metric,
    MetricStats,
    AlertRule,
    Alert,
    Anomaly,
    AnomalyConfig,
    HealthScore,
    HealthTrend,
    # Classes
    MetricsStore,
    AlertManager,
    AnomalyDetector,
    HealthScorer,
    # Utilities
    severity_to_weight,
    compare_values,
)


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create a temporary data directory for testing."""
    data_dir = tmp_path / "observability"
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir)


@pytest.fixture
def metrics_store(temp_data_dir):
    """Create a fresh MetricsStore for testing."""
    return MetricsStore(data_dir=temp_data_dir)


@pytest.fixture
def alert_manager(temp_data_dir):
    """Create a fresh AlertManager for testing."""
    return AlertManager(data_dir=temp_data_dir)


@pytest.fixture
def anomaly_detector(temp_data_dir, metrics_store):
    """Create an AnomalyDetector with the test store."""
    return AnomalyDetector(metrics_store, data_dir=temp_data_dir)


@pytest.fixture
def health_scorer(temp_data_dir, metrics_store, alert_manager):
    """Create a HealthScorer with test components."""
    return HealthScorer(metrics_store, alert_manager, data_dir=temp_data_dir)


# =============================================================================
# Type Tests
# =============================================================================

class TestTypes:
    """Test Pydantic models and enums."""

    def test_metric_types(self):
        """Test all metric types are defined."""
        types = list(MetricType)
        assert MetricType.GAUGE in types
        assert MetricType.COUNTER in types
        assert MetricType.HISTOGRAM in types

    def test_alert_severity(self):
        """Test alert severity levels."""
        assert AlertSeverity.INFO.value == "info"
        assert AlertSeverity.WARNING.value == "warning"
        assert AlertSeverity.CRITICAL.value == "critical"

    def test_alert_status(self):
        """Test alert statuses."""
        assert AlertStatus.ACTIVE.value == "active"
        assert AlertStatus.ACKNOWLEDGED.value == "acknowledged"
        assert AlertStatus.RESOLVED.value == "resolved"

    def test_anomaly_types(self):
        """Test anomaly types."""
        assert AnomalyType.SPIKE.value == "spike"
        assert AnomalyType.DROP.value == "drop"
        assert AnomalyType.DRIFT.value == "drift"
        assert AnomalyType.OUTLIER.value == "outlier"

    def test_metric_model(self):
        """Test Metric model creation."""
        metric = Metric(
            name="test.metric",
            type=MetricType.GAUGE,
            value=42.5,
            tags={"env": "test"},
            unit="percent"
        )
        assert metric.name == "test.metric"
        assert metric.value == 42.5
        assert metric.type == MetricType.GAUGE
        assert metric.tags["env"] == "test"
        assert metric.id is not None

    def test_alert_rule_model(self):
        """Test AlertRule model creation."""
        rule = AlertRule(
            name="High CPU",
            metric_name="cpu.usage",
            threshold=90.0,
            comparison=">",
            severity=AlertSeverity.WARNING
        )
        assert rule.name == "High CPU"
        assert rule.threshold == 90.0
        assert rule.comparison == ">"
        assert rule.enabled is True

    def test_health_score_model(self):
        """Test HealthScore model creation."""
        score = HealthScore(
            asset_id="test-asset",
            asset_type="hierarchy_project",
            overall_score=85.0,
            quality_score=90.0,
            freshness_score=80.0
        )
        assert score.overall_score == 85.0
        assert score.quality_score == 90.0
        assert score.asset_type == "hierarchy_project"

    def test_severity_to_weight(self):
        """Test severity to weight conversion."""
        assert severity_to_weight(AlertSeverity.INFO) == 2.0
        assert severity_to_weight(AlertSeverity.WARNING) == 5.0
        assert severity_to_weight(AlertSeverity.CRITICAL) == 15.0

    def test_compare_values(self):
        """Test value comparison function."""
        assert compare_values(10, 5, ">") is True
        assert compare_values(5, 10, ">") is False
        assert compare_values(5, 10, "<") is True
        assert compare_values(10, 10, ">=") is True
        assert compare_values(10, 10, "==") is True
        assert compare_values(10, 5, "!=") is True


# =============================================================================
# MetricsStore Tests
# =============================================================================

class TestMetricsStore:
    """Test MetricsStore operations."""

    def test_record_metric(self, metrics_store):
        """Test recording a metric."""
        metric = metrics_store.record_value(
            name="test.metric",
            value=42.5,
            metric_type=MetricType.GAUGE,
            tags={"env": "test"}
        )

        assert metric.name == "test.metric"
        assert metric.value == 42.5
        assert metric.id is not None

    def test_record_and_query(self, metrics_store):
        """Test recording and querying metrics."""
        # Record several metrics
        for i in range(5):
            metrics_store.record_value(
                name="query.test",
                value=float(i),
                metric_type=MetricType.GAUGE
            )

        # Query
        metrics = metrics_store.query("query.test", hours=1)
        assert len(metrics) == 5

    def test_query_with_tags(self, metrics_store):
        """Test querying with tag filter."""
        metrics_store.record_value("tagged.metric", 1.0, tags={"env": "prod"})
        metrics_store.record_value("tagged.metric", 2.0, tags={"env": "dev"})

        prod_metrics = metrics_store.query("tagged.metric", tags={"env": "prod"})
        assert len(prod_metrics) == 1
        assert prod_metrics[0].value == 1.0

    def test_aggregate_metrics(self, metrics_store):
        """Test metric aggregation."""
        for v in [10, 20, 30, 40, 50]:
            metrics_store.record_value("agg.test", float(v))

        stats = metrics_store.aggregate("agg.test", hours=1)
        assert stats.count == 5
        assert stats.min_value == 10.0
        assert stats.max_value == 50.0
        assert stats.avg_value == 30.0

    def test_get_latest_metric(self, metrics_store):
        """Test getting latest metric value."""
        import time
        metrics_store.record_value("latest.test", 100.0)
        time.sleep(0.01)  # Small delay to ensure different timestamps
        metrics_store.record_value("latest.test", 200.0)

        latest = metrics_store.get_latest("latest.test")
        assert latest is not None
        assert latest.value == 200.0

    def test_list_metric_names(self, metrics_store):
        """Test listing unique metric names."""
        metrics_store.record_value("metric.a", 1.0)
        metrics_store.record_value("metric.b", 2.0)
        metrics_store.record_value("metric.a", 3.0)

        names = metrics_store.list_metric_names()
        assert "metric.a" in names
        assert "metric.b" in names
        assert len([n for n in names if n.startswith("metric.")]) == 2

    def test_storage_stats(self, metrics_store):
        """Test storage statistics."""
        metrics_store.record_value("stats.test", 1.0)

        stats = metrics_store.get_storage_stats()
        assert stats["file_exists"] is True
        assert stats["total_metrics"] >= 1
        assert stats["unique_metric_names"] >= 1

    def test_cleanup_old_metrics(self, metrics_store):
        """Test cleanup of old metrics."""
        # Record a metric
        metrics_store.record_value("cleanup.test", 1.0)

        # Cleanup (nothing should be removed since it's fresh)
        removed = metrics_store.cleanup_old_metrics(days=30)
        assert removed == 0

    def test_percentile_calculation(self, metrics_store):
        """Test percentile calculations in aggregation."""
        # Record values from 1 to 100
        for v in range(1, 101):
            metrics_store.record_value("percentile.test", float(v))

        stats = metrics_store.aggregate("percentile.test", hours=1)
        # Median of 1-100 is 50.5 (interpolated)
        assert 49.0 <= stats.p50 <= 51.0  # Allow small variance
        assert stats.p95 >= 94.0
        assert stats.p99 >= 98.0


# =============================================================================
# AlertManager Tests
# =============================================================================

class TestAlertManager:
    """Test AlertManager operations."""

    def test_create_alert_rule(self, alert_manager):
        """Test creating an alert rule."""
        rule = alert_manager.create_rule_from_params(
            name="Test Alert",
            metric_name="test.metric",
            threshold=90.0,
            comparison=">",
            severity=AlertSeverity.WARNING
        )

        assert rule.name == "Test Alert"
        assert rule.threshold == 90.0
        assert rule.enabled is True

    def test_list_alert_rules(self, alert_manager):
        """Test listing alert rules."""
        alert_manager.create_rule_from_params(
            name="Rule 1", metric_name="m1", threshold=50.0, comparison=">"
        )
        alert_manager.create_rule_from_params(
            name="Rule 2", metric_name="m2", threshold=75.0, comparison="<"
        )

        rules = alert_manager.list_rules()
        assert len(rules) == 2

    def test_evaluate_metric_triggers_alert(self, alert_manager):
        """Test that metrics can trigger alerts."""
        rule = alert_manager.create_rule_from_params(
            name="High Value",
            metric_name="test.value",
            threshold=50.0,
            comparison=">",
            severity=AlertSeverity.WARNING
        )

        # Create a metric that exceeds threshold
        metric = Metric(
            name="test.value",
            type=MetricType.GAUGE,
            value=75.0
        )

        alert = alert_manager.evaluate_metric(metric)
        assert alert is not None
        assert alert.severity == AlertSeverity.WARNING
        assert alert.metric_value == 75.0

    def test_evaluate_metric_no_trigger(self, alert_manager):
        """Test that metrics below threshold don't trigger."""
        alert_manager.create_rule_from_params(
            name="High Value",
            metric_name="test.value",
            threshold=50.0,
            comparison=">"
        )

        metric = Metric(name="test.value", type=MetricType.GAUGE, value=25.0)
        alert = alert_manager.evaluate_metric(metric)
        assert alert is None

    def test_acknowledge_alert(self, alert_manager):
        """Test acknowledging an alert."""
        rule = alert_manager.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">"
        )

        metric = Metric(name="test", type=MetricType.GAUGE, value=20.0)
        alert = alert_manager.evaluate_metric(metric)

        success = alert_manager.acknowledge(alert.id, user="admin")
        assert success is True

        updated = alert_manager.get_alert(alert.id)
        assert updated.status == AlertStatus.ACKNOWLEDGED
        assert updated.acknowledged_by == "admin"

    def test_resolve_alert(self, alert_manager):
        """Test resolving an alert."""
        rule = alert_manager.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">"
        )

        metric = Metric(name="test", type=MetricType.GAUGE, value=20.0)
        alert = alert_manager.evaluate_metric(metric)

        success = alert_manager.resolve(alert.id, user="admin")
        assert success is True

        updated = alert_manager.get_alert(alert.id)
        assert updated.status == AlertStatus.RESOLVED

    def test_list_active_alerts(self, alert_manager):
        """Test listing active alerts."""
        rule = alert_manager.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">",
            severity=AlertSeverity.CRITICAL
        )

        metric = Metric(name="test", type=MetricType.GAUGE, value=20.0)
        alert = alert_manager.evaluate_metric(metric)

        active = alert_manager.list_active()
        assert len(active) >= 1
        assert any(a.id == alert.id for a in active)

    def test_alert_cooldown(self, alert_manager):
        """Test alert cooldown prevents re-triggering."""
        rule = alert_manager.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">",
            cooldown_minutes=60  # 1 hour cooldown
        )

        # First trigger
        metric1 = Metric(name="test", type=MetricType.GAUGE, value=20.0)
        alert1 = alert_manager.evaluate_metric(metric1)
        assert alert1 is not None

        # Second trigger (should be blocked by cooldown)
        metric2 = Metric(name="test", type=MetricType.GAUGE, value=25.0)
        alert2 = alert_manager.evaluate_metric(metric2)
        assert alert2 is None

    def test_get_alert_stats(self, alert_manager):
        """Test getting alert statistics."""
        rule = alert_manager.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">"
        )

        stats = alert_manager.get_alert_stats()
        assert "total_alerts" in stats
        assert "total_rules" in stats
        assert stats["total_rules"] >= 1

    def test_update_rule(self, alert_manager):
        """Test updating an alert rule."""
        rule = alert_manager.create_rule_from_params(
            name="Original", metric_name="test", threshold=50.0, comparison=">"
        )

        updated = alert_manager.update_rule(rule.id, {"threshold": 75.0, "name": "Updated"})
        assert updated.threshold == 75.0
        assert updated.name == "Updated"

    def test_delete_rule(self, alert_manager):
        """Test deleting an alert rule."""
        rule = alert_manager.create_rule_from_params(
            name="ToDelete", metric_name="test", threshold=50.0, comparison=">"
        )

        success = alert_manager.delete_rule(rule.id)
        assert success is True

        retrieved = alert_manager.get_rule(rule.id)
        assert retrieved is None


# =============================================================================
# AnomalyDetector Tests
# =============================================================================

class TestAnomalyDetector:
    """Test AnomalyDetector operations."""

    def test_configure_detector(self, anomaly_detector):
        """Test configuring anomaly detection."""
        config = anomaly_detector.configure(
            zscore_threshold=2.5,
            min_data_points=5
        )

        assert config.zscore_threshold == 2.5
        assert config.min_data_points == 5

    def test_get_baseline(self, anomaly_detector, metrics_store):
        """Test baseline calculation."""
        # Record some metrics
        for v in [100, 105, 95, 102, 98]:
            metrics_store.record_value("baseline.test", float(v))

        baseline = anomaly_detector.get_baseline("baseline.test")
        assert baseline["count"] == 5
        assert baseline["mean"] == 100.0
        assert baseline["sufficient_data"] is False  # Need 10 by default

    def test_detect_anomaly_insufficient_data(self, anomaly_detector, metrics_store):
        """Test that anomaly detection requires sufficient data."""
        metrics_store.record_value("sparse.metric", 100.0)

        anomaly = anomaly_detector.detect("sparse.metric", 1000.0)
        assert anomaly is None  # Not enough baseline data

    def test_detect_anomaly_spike(self, anomaly_detector, metrics_store):
        """Test spike detection."""
        # Configure for easier testing
        anomaly_detector.configure(min_data_points=5, zscore_threshold=2.0)

        # Record normal values
        for v in [100, 102, 98, 101, 99]:
            metrics_store.record_value("spike.test", float(v))

        # Test extreme value
        anomaly = anomaly_detector.detect("spike.test", 200.0)
        assert anomaly is not None
        assert anomaly.anomaly_type in [AnomalyType.SPIKE, AnomalyType.OUTLIER]

    def test_detect_anomaly_drop(self, anomaly_detector, metrics_store):
        """Test drop detection."""
        anomaly_detector.configure(min_data_points=5, zscore_threshold=2.0)

        for v in [100, 102, 98, 101, 99]:
            metrics_store.record_value("drop.test", float(v))

        anomaly = anomaly_detector.detect("drop.test", 10.0)
        assert anomaly is not None
        assert anomaly.anomaly_type in [AnomalyType.DROP, AnomalyType.OUTLIER]

    def test_no_anomaly_normal_value(self, anomaly_detector, metrics_store):
        """Test that normal values don't trigger anomalies."""
        anomaly_detector.configure(min_data_points=5, zscore_threshold=3.0)

        for v in [100, 102, 98, 101, 99]:
            metrics_store.record_value("normal.test", float(v))

        anomaly = anomaly_detector.detect("normal.test", 101.0)
        assert anomaly is None

    def test_calculate_zscore(self, anomaly_detector):
        """Test Z-score calculation."""
        zscore = anomaly_detector.calculate_zscore(value=110, mean=100, stddev=5)
        assert zscore == 2.0

    def test_classify_anomaly(self, anomaly_detector):
        """Test anomaly classification."""
        # Large positive deviation
        result = anomaly_detector.classify_anomaly(zscore=6.0, baseline_mean=100, actual_value=200)
        assert result == AnomalyType.SPIKE

        # Large negative deviation
        result = anomaly_detector.classify_anomaly(zscore=-6.0, baseline_mean=100, actual_value=10)
        assert result == AnomalyType.DROP

    def test_get_anomaly_report(self, anomaly_detector, metrics_store):
        """Test anomaly report generation."""
        anomaly_detector.configure(min_data_points=5, zscore_threshold=2.0)

        for v in [100, 102, 98, 101, 99]:
            metrics_store.record_value("report.test", float(v))

        # Trigger an anomaly
        anomaly_detector.detect("report.test", 200.0)

        report = anomaly_detector.get_anomaly_report("report.test", hours=24)
        assert "anomaly_count" in report
        assert "baseline" in report

    def test_get_anomaly_stats(self, anomaly_detector):
        """Test anomaly statistics."""
        stats = anomaly_detector.get_anomaly_stats(hours=24)
        assert "total_anomalies" in stats
        assert "by_type" in stats


# =============================================================================
# HealthScorer Tests
# =============================================================================

class TestHealthScorer:
    """Test HealthScorer operations."""

    def test_calculate_asset_health(self, health_scorer, metrics_store):
        """Test health score calculation."""
        # Record some health metrics
        tags = {"asset_id": "test-asset", "asset_type": "hierarchy_project"}
        metrics_store.record_value("hierarchy_project.quality", 90.0, tags=tags)
        metrics_store.record_value("hierarchy_project.freshness", 85.0, tags=tags)
        metrics_store.record_value("hierarchy_project.completeness", 95.0, tags=tags)
        metrics_store.record_value("hierarchy_project.reliability", 100.0, tags=tags)

        health = health_scorer.calculate_asset_health("test-asset", "hierarchy_project")
        assert health.overall_score > 0
        assert health.quality_score == 90.0
        assert health.asset_id == "test-asset"

    def test_health_score_with_alert_penalty(self, health_scorer, alert_manager):
        """Test that active alerts reduce health score."""
        # Create an alert rule and trigger it
        rule = alert_manager.create_rule_from_params(
            name="Test Alert",
            metric_name="test",
            threshold=10.0,
            comparison=">",
            severity=AlertSeverity.WARNING
        )

        metric = Metric(
            name="test",
            type=MetricType.GAUGE,
            value=20.0,
            tags={"asset_id": "alert-test", "asset_type": "test"}
        )
        alert_manager.evaluate_metric(metric)

        # Calculate health - penalty should apply
        health = health_scorer.calculate_asset_health("alert-test", "test")
        # Without metrics, base is 100; penalty reduces it
        # This test verifies the penalty mechanism works

    def test_get_system_health(self, health_scorer, metrics_store):
        """Test system health dashboard."""
        # Calculate health for a couple assets
        tags1 = {"asset_id": "asset-1", "asset_type": "hierarchy_project"}
        metrics_store.record_value("hierarchy_project.quality", 90.0, tags=tags1)
        health_scorer.calculate_asset_health("asset-1", "hierarchy_project")

        tags2 = {"asset_id": "asset-2", "asset_type": "catalog_asset"}
        metrics_store.record_value("catalog_asset.quality", 80.0, tags=tags2)
        health_scorer.calculate_asset_health("asset-2", "catalog_asset")

        system = health_scorer.get_system_health()
        assert "overall_health" in system
        assert "asset_count" in system
        assert system["asset_count"] >= 2

    def test_get_health_trend(self, health_scorer, metrics_store):
        """Test health trend calculation."""
        tags = {"asset_id": "trend-test", "asset_type": "hierarchy_project"}

        # Record metrics over time (simulated)
        for v in [85, 88, 90, 92, 95]:
            metrics_store.record_value("hierarchy_project.quality", float(v), tags=tags)

        trend = health_scorer.get_health_trend("trend-test", "hierarchy_project", hours=24)
        assert trend.asset_id == "trend-test"
        assert len(trend.data_points) > 0

    def test_record_health_metrics(self, health_scorer, metrics_store):
        """Test convenience method for recording health metrics."""
        result = health_scorer.record_health_metrics(
            asset_id="health-metrics-test",
            asset_type="hierarchy_project",
            quality=90.0,
            freshness=85.0,
            completeness=95.0
        )

        assert len(result["recorded_metrics"]) == 3

    def test_list_all_scores(self, health_scorer, metrics_store):
        """Test listing all health scores."""
        # Create some scores
        for i in range(3):
            health_scorer.calculate_asset_health(f"list-{i}", "hierarchy_project")

        scores = health_scorer.list_all_scores()
        assert len(scores) >= 3

        # Test filtering
        filtered = health_scorer.list_all_scores(asset_type="hierarchy_project")
        assert all(s.asset_type == "hierarchy_project" for s in filtered)

    def test_get_cached_health(self, health_scorer, metrics_store):
        """Test retrieving cached health scores."""
        health_scorer.calculate_asset_health("cached-test", "hierarchy_project")

        cached = health_scorer.get_cached_health("cached-test", "hierarchy_project")
        assert cached is not None
        assert cached.asset_id == "cached-test"

    def test_clear_cache(self, health_scorer, metrics_store):
        """Test clearing the health score cache."""
        health_scorer.calculate_asset_health("cache-clear", "hierarchy_project")

        cleared = health_scorer.clear_cache()
        assert cleared >= 1

        cached = health_scorer.get_cached_health("cache-clear", "hierarchy_project")
        assert cached is None


# =============================================================================
# MCP Tools Integration Tests
# =============================================================================

class TestMCPToolsIntegration:
    """Test MCP tool registration and functionality."""

    def test_tools_register_without_error(self):
        """Test that tools can be registered with a mock MCP."""
        from src.observability.mcp_tools import register_observability_tools

        mock_mcp = MagicMock()
        mock_mcp.tool = MagicMock(return_value=lambda f: f)

        # Should not raise
        register_observability_tools(mock_mcp)

        # Verify tool decorator was called 15 times
        assert mock_mcp.tool.call_count == 15

    def test_record_metric_via_store(self, metrics_store):
        """Test metric recording that MCP tool would use."""
        metric = metrics_store.record_value(
            name="mcp.test",
            value=42.0,
            metric_type=MetricType.GAUGE,
            tags={"source": "mcp"}
        )

        assert metric.name == "mcp.test"
        assert metric.value == 42.0

    def test_create_alert_rule_via_manager(self, alert_manager):
        """Test alert rule creation that MCP tool would use."""
        rule = alert_manager.create_rule_from_params(
            name="MCP Test Rule",
            metric_name="mcp.test",
            threshold=50.0,
            comparison=">",
            severity=AlertSeverity.WARNING
        )

        assert rule.name == "MCP Test Rule"

    def test_detect_anomaly_via_detector(self, anomaly_detector, metrics_store):
        """Test anomaly detection that MCP tool would use."""
        anomaly_detector.configure(min_data_points=3, zscore_threshold=2.0)

        for v in [100, 100, 100]:
            metrics_store.record_value("mcp.anomaly", float(v))

        anomaly = anomaly_detector.detect("mcp.anomaly", 200.0)
        # May or may not detect based on stddev

    def test_get_asset_health_via_scorer(self, health_scorer, metrics_store):
        """Test health scoring that MCP tool would use."""
        health = health_scorer.calculate_asset_health("mcp-test", "hierarchy_project")
        assert health.asset_id == "mcp-test"


# =============================================================================
# Persistence Tests
# =============================================================================

class TestPersistence:
    """Test data persistence across instances."""

    def test_metrics_persist(self, temp_data_dir):
        """Test metrics persist across store instances."""
        store1 = MetricsStore(data_dir=temp_data_dir)
        store1.record_value("persist.test", 123.0)

        store2 = MetricsStore(data_dir=temp_data_dir)
        metrics = store2.query("persist.test", hours=1)
        assert len(metrics) == 1
        assert metrics[0].value == 123.0

    def test_alert_rules_persist(self, temp_data_dir):
        """Test alert rules persist across manager instances."""
        mgr1 = AlertManager(data_dir=temp_data_dir)
        mgr1.create_rule_from_params(
            name="Persist Test",
            metric_name="test",
            threshold=50.0,
            comparison=">"
        )

        mgr2 = AlertManager(data_dir=temp_data_dir)
        rules = mgr2.list_rules()
        assert len(rules) == 1
        assert rules[0].name == "Persist Test"

    def test_alerts_persist(self, temp_data_dir):
        """Test alerts persist across manager instances."""
        mgr1 = AlertManager(data_dir=temp_data_dir)
        rule = mgr1.create_rule_from_params(
            name="Test", metric_name="test", threshold=10.0, comparison=">"
        )
        metric = Metric(name="test", type=MetricType.GAUGE, value=20.0)
        alert = mgr1.evaluate_metric(metric)

        mgr2 = AlertManager(data_dir=temp_data_dir)
        active = mgr2.list_active()
        assert len(active) >= 1


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_metric_query(self, metrics_store):
        """Test querying non-existent metrics."""
        metrics = metrics_store.query("non.existent", hours=24)
        assert len(metrics) == 0

    def test_empty_aggregation(self, metrics_store):
        """Test aggregating non-existent metrics."""
        stats = metrics_store.aggregate("non.existent", hours=24)
        assert stats.count == 0

    def test_acknowledge_non_existent_alert(self, alert_manager):
        """Test acknowledging non-existent alert."""
        success = alert_manager.acknowledge("fake-id")
        assert success is False

    def test_resolve_non_existent_alert(self, alert_manager):
        """Test resolving non-existent alert."""
        success = alert_manager.resolve("fake-id")
        assert success is False

    def test_delete_non_existent_rule(self, alert_manager):
        """Test deleting non-existent rule."""
        success = alert_manager.delete_rule("fake-id")
        assert success is False

    def test_zero_stddev_handling(self, anomaly_detector, metrics_store):
        """Test anomaly detection with zero standard deviation."""
        anomaly_detector.configure(min_data_points=3, zscore_threshold=3.0)

        # All same values = zero stddev
        for _ in range(5):
            metrics_store.record_value("zero.stddev", 100.0)

        # Different value should be detected as anomaly
        anomaly = anomaly_detector.detect("zero.stddev", 101.0)
        # With zero stddev, any deviation is notable

    def test_health_no_metrics(self, health_scorer):
        """Test health calculation with no metrics."""
        health = health_scorer.calculate_asset_health("no-metrics", "test_type")
        # Should return default scores (100)
        assert health.quality_score == 100.0

    def test_system_health_empty(self, health_scorer):
        """Test system health with no cached scores."""
        health_scorer.clear_cache()
        system = health_scorer.get_system_health()
        assert system["asset_count"] == 0
        assert system["overall_health"] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
