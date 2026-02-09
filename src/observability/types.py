"""
Data Observability Types - Pydantic models for metrics, alerting, and health scoring.

Phase 31: Real-time metrics collection, alerting, anomaly detection, and health monitoring.

Components:
- Metric: Time-series data point (gauge, counter, histogram)
- AlertRule: Threshold-based alert definition
- Alert: Active or historical alert instance
- Anomaly: Detected statistical anomaly
- HealthScore: Composite health score for assets
"""

from enum import Enum
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any, List
from datetime import datetime
import uuid


class MetricType(str, Enum):
    """Types of metrics that can be recorded."""
    GAUGE = "gauge"        # Point-in-time value (e.g., CPU usage, queue depth)
    COUNTER = "counter"    # Cumulative count (e.g., requests, errors)
    HISTOGRAM = "histogram" # Distribution (e.g., latency percentiles)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"          # Informational, no action needed
    WARNING = "warning"    # Potential issue, monitor closely
    CRITICAL = "critical"  # Immediate attention required


class AlertStatus(str, Enum):
    """Alert lifecycle statuses."""
    ACTIVE = "active"              # Alert is currently firing
    ACKNOWLEDGED = "acknowledged"  # Someone is looking at it
    RESOLVED = "resolved"          # Issue has been fixed


class AnomalyType(str, Enum):
    """Types of statistical anomalies."""
    SPIKE = "spike"      # Sudden increase
    DROP = "drop"        # Sudden decrease
    DRIFT = "drift"      # Gradual shift from baseline
    OUTLIER = "outlier"  # Single point far from mean


class Metric(BaseModel):
    """A single metric data point in the time series."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str  # e.g., "hierarchy.validation.success_rate"
    type: MetricType
    value: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = Field(default_factory=dict)  # e.g., {"project_id": "revenue-pl"}
    unit: str = ""  # e.g., "percent", "count", "ms"

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class MetricQuery(BaseModel):
    """Query parameters for metric search."""
    metric_name: str
    hours: int = 24
    tags: Optional[Dict[str, str]] = None
    limit: int = 1000


class MetricStats(BaseModel):
    """Aggregated statistics for a metric over a time range."""
    metric_name: str
    count: int = 0
    min_value: float = 0.0
    max_value: float = 0.0
    avg_value: float = 0.0
    sum_value: float = 0.0
    p50: float = 0.0  # Median
    p95: float = 0.0
    p99: float = 0.0
    stddev: float = 0.0
    first_timestamp: Optional[datetime] = None
    last_timestamp: Optional[datetime] = None


class AlertRule(BaseModel):
    """Threshold-based alert rule definition."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str  # e.g., "Low validation success rate"
    description: str = ""
    metric_name: str  # e.g., "hierarchy.validation.success_rate"
    threshold: float  # The value to compare against
    comparison: str  # ">", "<", ">=", "<=", "=="
    duration_minutes: int = 5  # How long condition must persist
    severity: AlertSeverity = AlertSeverity.WARNING
    enabled: bool = True
    tags: Dict[str, str] = Field(default_factory=dict)  # Filter metrics by tags
    notification_channels: List[str] = Field(default_factory=list)  # e.g., ["webhook", "console"]
    cooldown_minutes: int = 60  # Don't re-alert within this window
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class Alert(BaseModel):
    """An active or historical alert instance."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    rule_id: str
    rule_name: str
    status: AlertStatus = AlertStatus.ACTIVE
    severity: AlertSeverity
    metric_name: str
    metric_value: float
    threshold: float
    comparison: str
    triggered_at: datetime = Field(default_factory=datetime.utcnow)
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    message: str = ""
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class Anomaly(BaseModel):
    """Detected anomaly in metric data."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    metric_name: str
    anomaly_type: AnomalyType
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    baseline_value: float  # Expected value based on history
    actual_value: float    # Observed value
    zscore: float          # How many standard deviations from mean
    confidence: float      # 0-1, how confident we are this is anomalous
    window_hours: int = 168  # Time window used for baseline (default 1 week)
    message: str = ""
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class AnomalyConfig(BaseModel):
    """Configuration for anomaly detection."""
    zscore_threshold: float = 3.0  # Z-score threshold for anomaly detection
    min_data_points: int = 10      # Minimum data points needed for baseline
    baseline_hours: int = 168      # Hours of history to use (default 1 week)
    sensitivity: float = 1.0       # Multiplier for threshold (higher = less sensitive)


class HealthScore(BaseModel):
    """Composite health score for an asset."""
    asset_id: str
    asset_type: str  # e.g., "hierarchy_project", "catalog_asset", "semantic_model"
    overall_score: float  # 0-100, weighted composite

    # Component scores (0-100 each)
    quality_score: float = 100.0      # Data quality metrics
    freshness_score: float = 100.0    # How recent the data is
    completeness_score: float = 100.0 # Coverage and null rates
    reliability_score: float = 100.0  # Error rates and availability

    # Penalties
    alert_penalty: float = 0.0        # Deduction from active alerts
    anomaly_penalty: float = 0.0      # Deduction from recent anomalies

    # Metadata
    active_alerts: int = 0
    recent_anomalies: int = 0
    last_updated: Optional[datetime] = None
    calculated_at: datetime = Field(default_factory=datetime.utcnow)

    # Breakdown for transparency
    component_weights: Dict[str, float] = Field(default_factory=lambda: {
        "quality": 0.30,
        "freshness": 0.25,
        "completeness": 0.25,
        "reliability": 0.20
    })

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class HealthTrend(BaseModel):
    """Health score trend over time."""
    asset_id: str
    asset_type: str
    data_points: List[Dict[str, Any]] = Field(default_factory=list)  # [{timestamp, score}]
    trend_direction: str = "stable"  # "improving", "declining", "stable"
    change_percent: float = 0.0  # Change from start to end


class SLADefinition(BaseModel):
    """Service Level Agreement definition for an asset."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    asset_id: str
    asset_type: str

    # SLA targets
    min_health_score: float = 95.0     # Minimum acceptable health score
    max_downtime_minutes: int = 60     # Maximum downtime per period
    max_error_rate: float = 0.01       # Maximum error rate (1%)
    freshness_threshold_hours: int = 24  # Data must be this fresh

    # Tracking period
    period: str = "monthly"  # "daily", "weekly", "monthly"
    created_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class SLACompliance(BaseModel):
    """SLA compliance status for an asset."""
    sla_id: str
    sla_name: str
    asset_id: str
    period_start: datetime
    period_end: datetime

    # Compliance metrics
    is_compliant: bool = True
    health_score_avg: float = 100.0
    health_score_min: float = 100.0
    downtime_minutes: int = 0
    error_rate: float = 0.0
    violations: List[Dict[str, Any]] = Field(default_factory=list)

    calculated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = ConfigDict(ser_json_timedelta="iso8601")


# Utility functions

def severity_to_weight(severity: AlertSeverity) -> float:
    """Convert alert severity to penalty weight."""
    weights = {
        AlertSeverity.INFO: 2.0,
        AlertSeverity.WARNING: 5.0,
        AlertSeverity.CRITICAL: 15.0
    }
    return weights.get(severity, 5.0)


def compare_values(value: float, threshold: float, comparison: str) -> bool:
    """Compare a value against a threshold using the specified operator."""
    comparisons = {
        ">": lambda v, t: v > t,
        "<": lambda v, t: v < t,
        ">=": lambda v, t: v >= t,
        "<=": lambda v, t: v <= t,
        "==": lambda v, t: abs(v - t) < 0.0001,  # Float comparison with epsilon
        "!=": lambda v, t: abs(v - t) >= 0.0001
    }
    comparator = comparisons.get(comparison)
    if not comparator:
        raise ValueError(f"Invalid comparison operator: {comparison}")
    return comparator(value, threshold)
