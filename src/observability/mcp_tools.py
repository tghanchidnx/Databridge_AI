"""
Data Observability MCP Tools - Phase 31

15 tools for metrics collection, alerting, anomaly detection, and health scoring.

Metrics (4):
- obs_record_metric: Record a metric data point
- obs_query_metrics: Query metrics by name and time range
- obs_get_metric_stats: Get aggregated statistics for a metric
- obs_list_metrics: List all available metric names

Alerting (5):
- obs_create_alert_rule: Create a threshold-based alert rule
- obs_list_alert_rules: List all alert rules
- obs_list_active_alerts: List active (unresolved) alerts
- obs_acknowledge_alert: Acknowledge an alert
- obs_get_alert_history: Get historical alerts

Anomaly Detection (3):
- obs_detect_anomaly: Check if a value is anomalous
- obs_get_anomaly_report: Get recent anomalies for a metric
- obs_configure_anomaly: Set anomaly detection thresholds

Health Scoring (3):
- obs_get_asset_health: Get health score for an asset
- obs_get_system_health: Get overall system health dashboard
- obs_get_health_trends: Get health score trends over time
"""

import json
from typing import Optional
from .types import MetricType, AlertSeverity
from .metrics_store import MetricsStore
from .alert_manager import AlertManager
from .anomaly_detector import AnomalyDetector
from .health_scorer import HealthScorer

# Global instances
_store: MetricsStore = None
_alerts: AlertManager = None
_detector: AnomalyDetector = None
_scorer: HealthScorer = None


def get_components(data_dir: str = "data/observability"):
    """Get or initialize observability components."""
    global _store, _alerts, _detector, _scorer

    if _store is None:
        _store = MetricsStore(data_dir)
        _alerts = AlertManager(data_dir)
        _detector = AnomalyDetector(_store, data_dir)
        _scorer = HealthScorer(_store, _alerts, data_dir)

    return _store, _alerts, _detector, _scorer


def register_observability_tools(mcp, settings=None):
    """
    Register all observability tools with the MCP server.

    Args:
        mcp: FastMCP instance
        settings: Optional settings object with data_dir

    Returns:
        Tuple of (MetricsStore, AlertManager, AnomalyDetector, HealthScorer)
    """
    data_dir = "data/observability"
    if settings and hasattr(settings, "data_dir"):
        data_dir = f"{settings.data_dir}/observability"

    store, alerts, detector, scorer = get_components(data_dir)

    # =========================================================================
    # Metrics Tools (4)
    # =========================================================================

    @mcp.tool()
    def obs_record_metric(
        name: str,
        value: float,
        type: str = "gauge",
        tags: str = None,
        unit: str = ""
    ) -> dict:
        """
        Record a metric data point.

        Use this to capture time-series data for monitoring.
        Metrics are stored in append-only JSONL format.

        Args:
            name: Metric name (e.g., "hierarchy.validation.success_rate")
            value: Numeric value to record
            type: Metric type - "gauge" (point-in-time), "counter" (cumulative), "histogram"
            tags: Optional JSON object of tags for filtering (e.g., '{"project_id": "revenue-pl"}')
            unit: Optional unit (e.g., "percent", "ms", "count")

        Returns:
            The recorded metric with ID and timestamp

        Example:
            obs_record_metric(
                name="hierarchy.validation.success_rate",
                value=98.5,
                type="gauge",
                tags='{"project_id": "revenue-pl"}',
                unit="percent"
            )
        """
        try:
            metric_type = MetricType(type)
        except ValueError:
            return {"error": f"Invalid type. Valid: {[t.value for t in MetricType]}"}

        parsed_tags = {}
        if tags:
            try:
                parsed_tags = json.loads(tags)
            except json.JSONDecodeError:
                return {"error": "Invalid tags JSON"}

        metric = store.record_value(name, value, metric_type, parsed_tags, unit)

        # Also check for alert triggers
        alert = alerts.evaluate_metric(metric)

        result = metric.model_dump(mode="json")
        if alert:
            result["triggered_alert"] = {
                "id": alert.id,
                "rule_name": alert.rule_name,
                "severity": alert.severity.value,
                "message": alert.message
            }

        return result

    @mcp.tool()
    def obs_query_metrics(
        metric_name: str,
        hours: int = 24,
        tags: str = None,
        limit: int = 100
    ) -> dict:
        """
        Query metrics by name and time range.

        Args:
            metric_name: Name of the metric to query
            hours: Number of hours to look back (default 24)
            tags: Optional JSON filter for tags
            limit: Maximum results (default 100, max 1000)

        Returns:
            List of matching metrics with timestamps and values

        Example:
            obs_query_metrics(
                metric_name="hierarchy.validation.success_rate",
                hours=48,
                tags='{"project_id": "revenue-pl"}'
            )
        """
        parsed_tags = None
        if tags:
            try:
                parsed_tags = json.loads(tags)
            except json.JSONDecodeError:
                return {"error": "Invalid tags JSON"}

        limit = min(limit, 1000)
        metrics = store.query(metric_name, hours, parsed_tags, limit)

        return {
            "metric_name": metric_name,
            "hours": hours,
            "count": len(metrics),
            "metrics": [m.model_dump(mode="json") for m in metrics[:100]]  # Limit response size
        }

    @mcp.tool()
    def obs_get_metric_stats(metric_name: str, hours: int = 24) -> dict:
        """
        Get aggregated statistics for a metric.

        Calculates min, max, avg, percentiles, and standard deviation.

        Args:
            metric_name: Name of the metric
            hours: Time window in hours (default 24)

        Returns:
            Aggregated statistics including p50, p95, p99

        Example:
            obs_get_metric_stats("hierarchy.validation.success_rate", hours=168)
        """
        stats = store.aggregate(metric_name, hours)
        return stats.model_dump(mode="json")

    @mcp.tool()
    def obs_list_metrics() -> dict:
        """
        List all available metric names.

        Returns:
            List of unique metric names and storage statistics

        Example:
            obs_list_metrics()
        """
        names = store.list_metric_names()
        storage_stats = store.get_storage_stats()

        return {
            "metric_names": names,
            "count": len(names),
            "storage": storage_stats
        }

    # =========================================================================
    # Alerting Tools (5)
    # =========================================================================

    @mcp.tool()
    def obs_create_alert_rule(
        name: str,
        metric_name: str,
        threshold: float,
        comparison: str,
        severity: str = "warning",
        duration_minutes: int = 5,
        description: str = ""
    ) -> dict:
        """
        Create a threshold-based alert rule.

        When a metric value matches the condition, an alert is triggered.

        Args:
            name: Human-readable rule name
            metric_name: Metric to monitor
            threshold: Value to compare against
            comparison: Operator - ">", "<", ">=", "<=", "=="
            severity: Alert severity - "info", "warning", "critical"
            duration_minutes: How long condition must persist (default 5)
            description: Optional description

        Returns:
            The created alert rule

        Example:
            obs_create_alert_rule(
                name="Low validation success rate",
                metric_name="hierarchy.validation.success_rate",
                threshold=95.0,
                comparison="<",
                severity="warning"
            )
        """
        if comparison not in [">", "<", ">=", "<=", "==", "!="]:
            return {"error": f"Invalid comparison. Valid: >, <, >=, <=, ==, !="}

        try:
            sev = AlertSeverity(severity)
        except ValueError:
            return {"error": f"Invalid severity. Valid: {[s.value for s in AlertSeverity]}"}

        rule = alerts.create_rule_from_params(
            name=name,
            metric_name=metric_name,
            threshold=threshold,
            comparison=comparison,
            severity=sev,
            duration_minutes=duration_minutes,
            description=description
        )

        return rule.model_dump(mode="json")

    @mcp.tool()
    def obs_list_alert_rules(enabled_only: bool = False) -> dict:
        """
        List all alert rules.

        Args:
            enabled_only: If true, only show enabled rules

        Returns:
            List of alert rules with their configurations

        Example:
            obs_list_alert_rules(enabled_only=True)
        """
        rules = alerts.list_rules(enabled_only)
        return {
            "count": len(rules),
            "rules": [r.model_dump(mode="json") for r in rules]
        }

    @mcp.tool()
    def obs_list_active_alerts() -> dict:
        """
        List active (unresolved) alerts.

        Shows all alerts that are still active or acknowledged but not resolved.
        Sorted by severity (critical first) then by time.

        Returns:
            List of active alerts with severity and details

        Example:
            obs_list_active_alerts()
        """
        active = alerts.list_active()
        stats = alerts.get_alert_stats()

        return {
            "count": len(active),
            "alerts": [a.model_dump(mode="json") for a in active],
            "stats": stats
        }

    @mcp.tool()
    def obs_acknowledge_alert(alert_id: str, user: str = None) -> dict:
        """
        Acknowledge an active alert.

        Moves the alert from "active" to "acknowledged" status,
        indicating someone is investigating.

        Args:
            alert_id: ID of the alert to acknowledge
            user: Optional username acknowledging

        Returns:
            Success status and updated alert

        Example:
            obs_acknowledge_alert("abc123", user="admin")
        """
        success = alerts.acknowledge(alert_id, user)

        if not success:
            return {"error": "Alert not found or already resolved"}

        alert = alerts.get_alert(alert_id)
        return {
            "success": True,
            "message": "Alert acknowledged",
            "alert": alert.model_dump(mode="json") if alert else None
        }

    @mcp.tool()
    def obs_get_alert_history(hours: int = 24, include_resolved: bool = True) -> dict:
        """
        Get historical alerts.

        Args:
            hours: Time window in hours (default 24)
            include_resolved: Include resolved alerts (default true)

        Returns:
            List of historical alerts

        Example:
            obs_get_alert_history(hours=168, include_resolved=False)
        """
        history = alerts.get_history(hours)

        if not include_resolved:
            from .types import AlertStatus
            history = [a for a in history if a.status != AlertStatus.RESOLVED]

        return {
            "hours": hours,
            "count": len(history),
            "alerts": [a.model_dump(mode="json") for a in history]
        }

    # =========================================================================
    # Anomaly Detection Tools (3)
    # =========================================================================

    @mcp.tool()
    def obs_detect_anomaly(metric_name: str, value: float, tags: str = None) -> dict:
        """
        Check if a value is anomalous based on historical data.

        Uses Z-score analysis to detect if a value significantly
        deviates from the historical baseline.

        Args:
            metric_name: Name of the metric
            value: Value to check
            tags: Optional JSON tags for context

        Returns:
            Anomaly details if detected, or baseline info if normal

        Example:
            obs_detect_anomaly(
                metric_name="hierarchy.validation.success_rate",
                value=72.0
            )
        """
        parsed_tags = {}
        if tags:
            try:
                parsed_tags = json.loads(tags)
            except json.JSONDecodeError:
                return {"error": "Invalid tags JSON"}

        anomaly = detector.detect(metric_name, value, parsed_tags)
        baseline = detector.get_baseline(metric_name)

        if anomaly:
            return {
                "is_anomaly": True,
                "anomaly": anomaly.model_dump(mode="json"),
                "baseline": baseline
            }
        else:
            return {
                "is_anomaly": False,
                "value": value,
                "baseline": baseline,
                "message": "Value is within normal range"
            }

    @mcp.tool()
    def obs_get_anomaly_report(metric_name: str, hours: int = 24) -> dict:
        """
        Get recent anomalies for a metric.

        Shows detected anomalies and baseline statistics.

        Args:
            metric_name: Name of the metric
            hours: Time window in hours (default 24)

        Returns:
            Anomaly report with counts by type and recent detections

        Example:
            obs_get_anomaly_report("hierarchy.validation.success_rate", hours=168)
        """
        return detector.get_anomaly_report(metric_name, hours)

    @mcp.tool()
    def obs_configure_anomaly(
        zscore_threshold: float = None,
        min_data_points: int = None,
        baseline_hours: int = None,
        sensitivity: float = None
    ) -> dict:
        """
        Configure anomaly detection thresholds.

        Args:
            zscore_threshold: Z-score threshold (default 3.0, higher = less sensitive)
            min_data_points: Minimum data points for baseline (default 10)
            baseline_hours: Hours of history for baseline (default 168 = 1 week)
            sensitivity: Multiplier for threshold (default 1.0, higher = less sensitive)

        Returns:
            Updated configuration

        Example:
            obs_configure_anomaly(zscore_threshold=2.5, sensitivity=0.8)
        """
        config = detector.configure(
            zscore_threshold=zscore_threshold,
            min_data_points=min_data_points,
            baseline_hours=baseline_hours,
            sensitivity=sensitivity
        )
        return {
            "message": "Anomaly detection configured",
            "config": config.model_dump()
        }

    # =========================================================================
    # Health Scoring Tools (3)
    # =========================================================================

    @mcp.tool()
    def obs_get_asset_health(asset_id: str, asset_type: str) -> dict:
        """
        Get health score for an asset.

        Calculates a composite health score (0-100) based on:
        - Quality score (30%)
        - Freshness score (25%)
        - Completeness score (25%)
        - Reliability score (20%)
        - Penalties from active alerts and anomalies

        Args:
            asset_id: Unique identifier for the asset
            asset_type: Type of asset (e.g., "hierarchy_project", "catalog_asset")

        Returns:
            Comprehensive health score with component breakdown

        Example:
            obs_get_asset_health(
                asset_id="revenue-pl",
                asset_type="hierarchy_project"
            )
        """
        health = scorer.calculate_asset_health(asset_id, asset_type)
        return health.model_dump(mode="json")

    @mcp.tool()
    def obs_get_system_health() -> dict:
        """
        Get overall system health dashboard.

        Aggregates health scores across all monitored assets.

        Returns:
            System health overview with:
            - Overall health percentage
            - Counts by health status (healthy/degraded/critical)
            - Active alerts summary
            - Lowest scoring assets

        Example:
            obs_get_system_health()
        """
        return scorer.get_system_health()

    @mcp.tool()
    def obs_get_health_trends(
        asset_id: str,
        asset_type: str,
        hours: int = 168
    ) -> dict:
        """
        Get health score trends over time.

        Shows how an asset's health has changed and the trend direction.

        Args:
            asset_id: Asset identifier
            asset_type: Asset type
            hours: Time window in hours (default 168 = 1 week)

        Returns:
            Trend data with direction (improving/declining/stable)

        Example:
            obs_get_health_trends(
                asset_id="revenue-pl",
                asset_type="hierarchy_project",
                hours=720
            )
        """
        trend = scorer.get_health_trend(asset_id, asset_type, hours)
        return trend.model_dump(mode="json")

    print("Data Observability tools registered (15 tools)")
    return store, alerts, detector, scorer
