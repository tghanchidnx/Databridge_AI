"""
Data Observability Module - Phase 31

Real-time metrics collection, alerting, anomaly detection, and health monitoring
for DataBridge objects.

Key Components:
- MetricsStore: Time-series storage using JSONL append-only format
- AlertManager: Threshold-based alerting with severity levels
- AnomalyDetector: Statistical anomaly detection using Z-scores
- HealthScorer: Composite health scoring for assets

Integrations:
- Data Quality: Feed validation failures into alerts
- Lineage: Track when changes cause metric shifts
- Console Dashboard: Stream alerts/metrics real-time
- Data Catalog: Tag assets with health scores

MCP Tools (15):
- Metrics: obs_record_metric, obs_query_metrics, obs_get_metric_stats, obs_list_metrics
- Alerting: obs_create_alert_rule, obs_list_alert_rules, obs_list_active_alerts,
            obs_acknowledge_alert, obs_get_alert_history
- Anomaly: obs_detect_anomaly, obs_get_anomaly_report, obs_configure_anomaly
- Health: obs_get_asset_health, obs_get_system_health, obs_get_health_trends

Example Usage:
    # Record a metric
    obs_record_metric(
        name="hierarchy.validation.success_rate",
        value=98.5,
        type="gauge",
        tags='{"project_id": "revenue-pl"}'
    )

    # Create an alert rule
    obs_create_alert_rule(
        name="Low validation success rate",
        metric_name="hierarchy.validation.success_rate",
        threshold=95.0,
        comparison="<",
        severity="warning"
    )

    # Check for anomalies
    obs_detect_anomaly(
        metric_name="hierarchy.validation.success_rate",
        value=72.0
    )

    # Get asset health
    obs_get_asset_health(
        asset_id="revenue-pl",
        asset_type="hierarchy_project"
    )
"""

from .types import (
    # Enums
    MetricType,
    AlertSeverity,
    AlertStatus,
    AnomalyType,
    # Models
    Metric,
    MetricQuery,
    MetricStats,
    AlertRule,
    Alert,
    Anomaly,
    AnomalyConfig,
    HealthScore,
    HealthTrend,
    SLADefinition,
    SLACompliance,
    # Utility functions
    severity_to_weight,
    compare_values,
)
from .metrics_store import MetricsStore
from .alert_manager import AlertManager
from .anomaly_detector import AnomalyDetector
from .health_scorer import HealthScorer
from .mcp_tools import register_observability_tools

__all__ = [
    # Enums
    "MetricType",
    "AlertSeverity",
    "AlertStatus",
    "AnomalyType",
    # Models
    "Metric",
    "MetricQuery",
    "MetricStats",
    "AlertRule",
    "Alert",
    "Anomaly",
    "AnomalyConfig",
    "HealthScore",
    "HealthTrend",
    "SLADefinition",
    "SLACompliance",
    # Core classes
    "MetricsStore",
    "AlertManager",
    "AnomalyDetector",
    "HealthScorer",
    # Utilities
    "severity_to_weight",
    "compare_values",
    # MCP registration
    "register_observability_tools",
]
