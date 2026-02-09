"""
Metrics Store - Time-series storage for observability metrics.

Uses JSONL append-only format for durability and easy recovery.
Supports querying by metric name, time range, and tags.
"""

import json
import statistics
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from .types import Metric, MetricType, MetricStats


class MetricsStore:
    """Time-series storage for metrics using JSONL append-only files."""

    def __init__(self, data_dir: str = "data/observability"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_file = self.data_dir / "metrics.jsonl"
        self._metric_names_cache: set = set()
        self._cache_loaded = False

    def record(self, metric: Metric) -> Metric:
        """
        Append a metric data point to storage.

        Args:
            metric: The Metric object to record

        Returns:
            The recorded metric with its generated ID
        """
        # Ensure timestamp is set
        if metric.timestamp is None:
            metric.timestamp = datetime.utcnow()

        # Append to JSONL file
        with open(self.metrics_file, "a", encoding="utf-8") as f:
            f.write(metric.model_dump_json() + "\n")

        # Update cache
        self._metric_names_cache.add(metric.name)

        return metric

    def record_value(
        self,
        name: str,
        value: float,
        metric_type: MetricType = MetricType.GAUGE,
        tags: Optional[Dict[str, str]] = None,
        unit: str = ""
    ) -> Metric:
        """
        Convenience method to record a metric value directly.

        Args:
            name: Metric name (e.g., "hierarchy.validation.success_rate")
            value: The numeric value
            metric_type: Type of metric (gauge, counter, histogram)
            tags: Optional tags for filtering
            unit: Optional unit (e.g., "percent", "ms")

        Returns:
            The recorded Metric object
        """
        metric = Metric(
            name=name,
            type=metric_type,
            value=value,
            tags=tags or {},
            unit=unit
        )
        return self.record(metric)

    def query(
        self,
        metric_name: str,
        hours: int = 24,
        tags: Optional[Dict[str, str]] = None,
        limit: int = 1000
    ) -> List[Metric]:
        """
        Query metrics by name and time range.

        Args:
            metric_name: Name of the metric to query
            hours: Number of hours to look back
            tags: Optional tag filter (all tags must match)
            limit: Maximum number of results

        Returns:
            List of matching Metric objects, sorted by timestamp descending
        """
        if not self.metrics_file.exists():
            return []

        cutoff = datetime.utcnow() - timedelta(hours=hours)
        results = []

        with open(self.metrics_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if data.get("name") != metric_name:
                        continue

                    # Parse timestamp
                    ts_str = data.get("timestamp")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts.tzinfo:
                            ts = ts.replace(tzinfo=None)  # Make naive for comparison
                        if ts < cutoff:
                            continue

                    # Check tags if specified
                    if tags:
                        metric_tags = data.get("tags", {})
                        if not all(metric_tags.get(k) == v for k, v in tags.items()):
                            continue

                    results.append(Metric(**data))
                except (json.JSONDecodeError, ValueError):
                    continue

        # Sort by timestamp descending and limit
        results.sort(key=lambda m: m.timestamp, reverse=True)
        return results[:limit]

    def aggregate(
        self,
        metric_name: str,
        hours: int = 24,
        tags: Optional[Dict[str, str]] = None
    ) -> MetricStats:
        """
        Calculate aggregated statistics for a metric.

        Args:
            metric_name: Name of the metric
            hours: Time window in hours
            tags: Optional tag filter

        Returns:
            MetricStats with min, max, avg, percentiles, etc.
        """
        metrics = self.query(metric_name, hours, tags)
        values = [m.value for m in metrics]

        if not values:
            return MetricStats(metric_name=metric_name)

        # Sort values for percentile calculation
        sorted_values = sorted(values)
        n = len(sorted_values)

        def percentile(p: float) -> float:
            """Calculate percentile from sorted values."""
            if n == 1:
                return sorted_values[0]
            k = (n - 1) * p / 100
            f = int(k)
            c = f + 1 if f + 1 < n else f
            return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])

        # Calculate standard deviation
        try:
            stddev = statistics.stdev(values) if len(values) > 1 else 0.0
        except statistics.StatisticsError:
            stddev = 0.0

        return MetricStats(
            metric_name=metric_name,
            count=len(values),
            min_value=min(values),
            max_value=max(values),
            avg_value=sum(values) / len(values),
            sum_value=sum(values),
            p50=percentile(50),
            p95=percentile(95),
            p99=percentile(99),
            stddev=stddev,
            first_timestamp=min(m.timestamp for m in metrics),
            last_timestamp=max(m.timestamp for m in metrics)
        )

    def get_latest(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> Optional[Metric]:
        """
        Get the most recent value for a metric.

        Args:
            metric_name: Name of the metric
            tags: Optional tag filter

        Returns:
            The most recent Metric or None
        """
        metrics = self.query(metric_name, hours=24*365, tags=tags, limit=1)
        return metrics[0] if metrics else None

    def list_metric_names(self) -> List[str]:
        """
        List all unique metric names in storage.

        Returns:
            Sorted list of metric names
        """
        if not self._cache_loaded and self.metrics_file.exists():
            with open(self.metrics_file, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        if "name" in data:
                            self._metric_names_cache.add(data["name"])
                    except json.JSONDecodeError:
                        continue
            self._cache_loaded = True

        return sorted(self._metric_names_cache)

    def get_metric_count(self, metric_name: str = None) -> int:
        """
        Get count of metrics in storage.

        Args:
            metric_name: Optional filter by metric name

        Returns:
            Number of metric data points
        """
        if not self.metrics_file.exists():
            return 0

        count = 0
        with open(self.metrics_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                if metric_name:
                    try:
                        data = json.loads(line)
                        if data.get("name") == metric_name:
                            count += 1
                    except json.JSONDecodeError:
                        continue
                else:
                    count += 1
        return count

    def cleanup_old_metrics(self, days: int = 30) -> int:
        """
        Remove metrics older than specified days.
        Creates a new file with only recent metrics.

        Args:
            days: Keep metrics from the last N days

        Returns:
            Number of metrics removed
        """
        if not self.metrics_file.exists():
            return 0

        cutoff = datetime.utcnow() - timedelta(days=days)
        temp_file = self.data_dir / "metrics_temp.jsonl"
        removed = 0
        kept = 0

        with open(self.metrics_file, "r", encoding="utf-8") as f_in, \
             open(temp_file, "w", encoding="utf-8") as f_out:
            for line in f_in:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    ts_str = data.get("timestamp")
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if ts.tzinfo:
                            ts = ts.replace(tzinfo=None)
                        if ts >= cutoff:
                            f_out.write(line)
                            kept += 1
                        else:
                            removed += 1
                    else:
                        f_out.write(line)
                        kept += 1
                except (json.JSONDecodeError, ValueError):
                    continue

        # Replace original with cleaned file
        temp_file.replace(self.metrics_file)

        # Rebuild cache
        self._cache_loaded = False
        self._metric_names_cache.clear()

        return removed

    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics.

        Returns:
            Dict with file size, metric count, unique names, etc.
        """
        stats = {
            "file_path": str(self.metrics_file),
            "file_exists": self.metrics_file.exists(),
            "file_size_bytes": 0,
            "file_size_mb": 0.0,
            "total_metrics": 0,
            "unique_metric_names": 0
        }

        if self.metrics_file.exists():
            stats["file_size_bytes"] = self.metrics_file.stat().st_size
            stats["file_size_mb"] = round(stats["file_size_bytes"] / (1024 * 1024), 2)
            stats["total_metrics"] = self.get_metric_count()
            stats["unique_metric_names"] = len(self.list_metric_names())

        return stats
