"""
Anomaly Detector - Statistical anomaly detection using Z-scores.

Detects spikes, drops, drift, and outliers in metric time series
using rolling baseline calculations.
"""

import json
import statistics
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from .types import Anomaly, AnomalyType, AnomalyConfig
from .metrics_store import MetricsStore


class AnomalyDetector:
    """Statistical anomaly detection using Z-scores."""

    def __init__(self, metrics_store: MetricsStore, data_dir: str = "data/observability"):
        self.metrics = metrics_store
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.anomalies_file = self.data_dir / "anomalies.jsonl"
        self.config_file = self.data_dir / "anomaly_config.json"
        self._config = self._load_config()

    def _load_config(self) -> AnomalyConfig:
        """Load configuration from disk or use defaults."""
        if self.config_file.exists():
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return AnomalyConfig(**data)
            except (json.JSONDecodeError, ValueError):
                pass
        return AnomalyConfig()

    def _save_config(self) -> None:
        """Persist configuration to disk."""
        with open(self.config_file, "w", encoding="utf-8") as f:
            json.dump(self._config.model_dump(), f, indent=2)

    def configure(
        self,
        zscore_threshold: float = None,
        min_data_points: int = None,
        baseline_hours: int = None,
        sensitivity: float = None
    ) -> AnomalyConfig:
        """
        Configure anomaly detection parameters.

        Args:
            zscore_threshold: Z-score threshold (default 3.0)
            min_data_points: Minimum data points for baseline (default 10)
            baseline_hours: Hours of history for baseline (default 168 = 1 week)
            sensitivity: Threshold multiplier (higher = less sensitive)

        Returns:
            Updated AnomalyConfig
        """
        if zscore_threshold is not None:
            self._config.zscore_threshold = zscore_threshold
        if min_data_points is not None:
            self._config.min_data_points = min_data_points
        if baseline_hours is not None:
            self._config.baseline_hours = baseline_hours
        if sensitivity is not None:
            self._config.sensitivity = sensitivity
        self._save_config()
        return self._config

    def get_config(self) -> AnomalyConfig:
        """Get current configuration."""
        return self._config

    def get_baseline(self, metric_name: str, hours: int = None) -> Dict[str, Any]:
        """
        Calculate baseline statistics from historical data.

        Args:
            metric_name: Name of the metric
            hours: Hours of history to use (default from config)

        Returns:
            Dict with mean, stddev, min, max, count
        """
        hours = hours or self._config.baseline_hours
        stats = self.metrics.aggregate(metric_name, hours=hours)

        return {
            "metric_name": metric_name,
            "window_hours": hours,
            "count": stats.count,
            "mean": stats.avg_value,
            "stddev": stats.stddev,
            "min": stats.min_value,
            "max": stats.max_value,
            "p50": stats.p50,
            "p95": stats.p95,
            "p99": stats.p99,
            "sufficient_data": stats.count >= self._config.min_data_points
        }

    def calculate_zscore(self, value: float, mean: float, stddev: float) -> float:
        """
        Calculate Z-score for a value.

        Args:
            value: The observed value
            mean: Population mean
            stddev: Population standard deviation

        Returns:
            Z-score (number of standard deviations from mean)
        """
        if stddev == 0 or stddev is None:
            return 0.0
        return (value - mean) / stddev

    def classify_anomaly(
        self,
        zscore: float,
        baseline_mean: float,
        actual_value: float
    ) -> AnomalyType:
        """
        Classify the type of anomaly based on direction and magnitude.

        Args:
            zscore: The calculated Z-score
            baseline_mean: The baseline mean value
            actual_value: The observed value

        Returns:
            AnomalyType classification
        """
        # Determine direction
        is_above = actual_value > baseline_mean

        # Classify based on Z-score magnitude and direction
        abs_zscore = abs(zscore)

        if abs_zscore > 5:
            # Extreme deviation - likely a spike or drop
            return AnomalyType.SPIKE if is_above else AnomalyType.DROP
        elif abs_zscore > 3:
            # Significant deviation
            return AnomalyType.OUTLIER
        else:
            # Moderate deviation - could be drift
            return AnomalyType.DRIFT

    def detect(
        self,
        metric_name: str,
        value: float,
        tags: Dict[str, str] = None
    ) -> Optional[Anomaly]:
        """
        Detect if a value is anomalous based on historical data.

        Args:
            metric_name: Name of the metric
            value: Value to check
            tags: Optional tags for context

        Returns:
            Anomaly object if anomalous, None otherwise
        """
        baseline = self.get_baseline(metric_name)

        # Need sufficient data for meaningful detection
        if not baseline["sufficient_data"]:
            return None

        mean = baseline["mean"]
        stddev = baseline["stddev"]

        # Handle zero or near-zero standard deviation
        if stddev < 0.0001:
            # If stddev is essentially 0, any deviation is notable
            if abs(value - mean) > 0.0001:
                zscore = 999.0 if value > mean else -999.0
            else:
                return None
        else:
            zscore = self.calculate_zscore(value, mean, stddev)

        # Apply sensitivity adjustment
        effective_threshold = self._config.zscore_threshold * self._config.sensitivity

        # Check if anomalous
        if abs(zscore) < effective_threshold:
            return None

        # Classify and create anomaly
        anomaly_type = self.classify_anomaly(zscore, mean, value)

        # Calculate confidence (higher Z-score = higher confidence)
        # Normalize to 0-1 range using sigmoid-like function
        confidence = min(1.0, abs(zscore) / (effective_threshold * 2))

        anomaly = Anomaly(
            metric_name=metric_name,
            anomaly_type=anomaly_type,
            baseline_value=mean,
            actual_value=value,
            zscore=round(zscore, 4),
            confidence=round(confidence, 4),
            window_hours=self._config.baseline_hours,
            message=f"{anomaly_type.value} detected: {metric_name} = {value} (baseline: {mean:.2f}, z-score: {zscore:.2f})",
            tags=tags or {}
        )

        # Log the anomaly
        self._log_anomaly(anomaly)

        return anomaly

    def _log_anomaly(self, anomaly: Anomaly) -> None:
        """Append anomaly to log file."""
        with open(self.anomalies_file, "a", encoding="utf-8") as f:
            f.write(anomaly.model_dump_json() + "\n")

    def get_recent_anomalies(
        self,
        metric_name: str = None,
        hours: int = 24,
        limit: int = 100
    ) -> List[Anomaly]:
        """
        Get recent anomalies from the log.

        Args:
            metric_name: Optional filter by metric name
            hours: Time window in hours
            limit: Maximum results

        Returns:
            List of Anomaly objects
        """
        if not self.anomalies_file.exists():
            return []

        cutoff = datetime.utcnow() - timedelta(hours=hours)
        results = []

        with open(self.anomalies_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)

                    # Filter by metric name if specified
                    if metric_name and data.get("metric_name") != metric_name:
                        continue

                    # Parse and filter by time
                    ts_str = data.get("detected_at")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts.tzinfo:
                            ts = ts.replace(tzinfo=None)
                        if ts < cutoff:
                            continue

                    results.append(Anomaly(**data))
                except (json.JSONDecodeError, ValueError):
                    continue

        # Sort by time descending
        results.sort(key=lambda a: a.detected_at, reverse=True)
        return results[:limit]

    def get_anomaly_report(
        self,
        metric_name: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """
        Generate an anomaly report for a metric.

        Args:
            metric_name: Name of the metric
            hours: Time window in hours

        Returns:
            Dict with anomaly statistics and details
        """
        anomalies = self.get_recent_anomalies(metric_name, hours)
        baseline = self.get_baseline(metric_name)

        # Count by type
        type_counts = {}
        for anomaly in anomalies:
            t = anomaly.anomaly_type.value
            type_counts[t] = type_counts.get(t, 0) + 1

        return {
            "metric_name": metric_name,
            "window_hours": hours,
            "baseline": baseline,
            "anomaly_count": len(anomalies),
            "by_type": type_counts,
            "recent_anomalies": [
                {
                    "id": a.id,
                    "type": a.anomaly_type.value,
                    "detected_at": a.detected_at.isoformat(),
                    "value": a.actual_value,
                    "zscore": a.zscore,
                    "confidence": a.confidence
                }
                for a in anomalies[:10]  # Limit preview
            ],
            "config": self._config.model_dump()
        }

    def get_anomaly_stats(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get overall anomaly statistics.

        Args:
            hours: Time window in hours

        Returns:
            Dict with counts by type, affected metrics, etc.
        """
        anomalies = self.get_recent_anomalies(hours=hours)

        # Group by metric
        by_metric = {}
        for a in anomalies:
            by_metric[a.metric_name] = by_metric.get(a.metric_name, 0) + 1

        # Count by type
        by_type = {}
        for a in anomalies:
            t = a.anomaly_type.value
            by_type[t] = by_type.get(t, 0) + 1

        return {
            "window_hours": hours,
            "total_anomalies": len(anomalies),
            "affected_metrics": len(by_metric),
            "by_type": by_type,
            "by_metric": dict(sorted(by_metric.items(), key=lambda x: x[1], reverse=True)[:10]),
            "config": self._config.model_dump()
        }
