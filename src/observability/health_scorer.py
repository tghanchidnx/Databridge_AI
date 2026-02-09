"""
Health Scorer - Composite health scoring for DataBridge assets.

Calculates weighted health scores based on quality, freshness,
completeness, reliability, and applies penalties for active alerts
and anomalies.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from .types import HealthScore, HealthTrend, AlertSeverity, severity_to_weight
from .metrics_store import MetricsStore
from .alert_manager import AlertManager


class HealthScorer:
    """Calculate composite health scores for assets."""

    # Default weights for health components
    DEFAULT_WEIGHTS = {
        "quality": 0.30,
        "freshness": 0.25,
        "completeness": 0.25,
        "reliability": 0.20
    }

    def __init__(
        self,
        metrics_store: MetricsStore,
        alert_manager: AlertManager,
        data_dir: str = "data/observability"
    ):
        self.metrics = metrics_store
        self.alerts = alert_manager
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.scores_file = self.data_dir / "health_scores.json"
        self._scores_cache: Dict[str, HealthScore] = {}
        self._load_cache()

    def _load_cache(self) -> None:
        """Load cached health scores."""
        if self.scores_file.exists():
            try:
                with open(self.scores_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for score_data in data.get("scores", []):
                        score = HealthScore(**score_data)
                        cache_key = f"{score.asset_type}:{score.asset_id}"
                        self._scores_cache[cache_key] = score
            except (json.JSONDecodeError, ValueError):
                pass

    def _save_cache(self) -> None:
        """Persist health scores to disk."""
        data = {
            "scores": [s.model_dump(mode="json") for s in self._scores_cache.values()],
            "updated_at": datetime.utcnow().isoformat()
        }
        with open(self.scores_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def _get_metric_score(
        self,
        metric_name: str,
        asset_id: str,
        default: float = 100.0
    ) -> float:
        """
        Get the latest metric value as a score (0-100).

        Args:
            metric_name: Full or partial metric name
            asset_id: Asset ID to filter by
            default: Default value if no metric found

        Returns:
            Score value (0-100)
        """
        # Try to get metric with asset tag
        metric = self.metrics.get_latest(metric_name, tags={"asset_id": asset_id})
        if metric:
            # Ensure value is in 0-100 range
            return max(0, min(100, metric.value))

        # Try without tag filter
        metric = self.metrics.get_latest(metric_name)
        if metric:
            return max(0, min(100, metric.value))

        return default

    def _calculate_alert_penalty(self, asset_id: str, asset_type: str) -> tuple:
        """
        Calculate penalty from active alerts for this asset.

        Args:
            asset_id: Asset identifier
            asset_type: Asset type

        Returns:
            Tuple of (penalty_points, active_alert_count)
        """
        # Get all active alerts
        active_alerts = self.alerts.list_active()

        # Filter for this asset (check tags)
        asset_alerts = [
            a for a in active_alerts
            if a.tags.get("asset_id") == asset_id or a.tags.get("asset_type") == asset_type
        ]

        if not asset_alerts:
            return 0.0, 0

        # Calculate penalty based on severity
        penalty = sum(severity_to_weight(a.severity) for a in asset_alerts)
        return min(penalty, 50.0), len(asset_alerts)  # Cap at 50 points

    def _calculate_anomaly_penalty(self, asset_id: str, hours: int = 24) -> tuple:
        """
        Calculate penalty from recent anomalies.

        Args:
            asset_id: Asset identifier
            hours: Time window for anomalies

        Returns:
            Tuple of (penalty_points, anomaly_count)
        """
        # This would integrate with AnomalyDetector
        # For now, return 0 - can be enhanced later
        return 0.0, 0

    def calculate_asset_health(
        self,
        asset_id: str,
        asset_type: str,
        weights: Dict[str, float] = None
    ) -> HealthScore:
        """
        Calculate comprehensive health score for an asset.

        Args:
            asset_id: Unique identifier for the asset
            asset_type: Type of asset (hierarchy_project, catalog_asset, etc.)
            weights: Optional custom weights for components

        Returns:
            HealthScore with overall and component scores
        """
        weights = weights or self.DEFAULT_WEIGHTS

        # Get component scores from metrics
        # Metric naming convention: {asset_type}.{component}.{asset_id}
        prefix = f"{asset_type}"

        quality_score = self._get_metric_score(f"{prefix}.quality", asset_id)
        freshness_score = self._get_metric_score(f"{prefix}.freshness", asset_id)
        completeness_score = self._get_metric_score(f"{prefix}.completeness", asset_id)
        reliability_score = self._get_metric_score(f"{prefix}.reliability", asset_id)

        # Calculate weighted average
        weighted_score = (
            quality_score * weights.get("quality", 0.30) +
            freshness_score * weights.get("freshness", 0.25) +
            completeness_score * weights.get("completeness", 0.25) +
            reliability_score * weights.get("reliability", 0.20)
        )

        # Apply penalties
        alert_penalty, active_alerts = self._calculate_alert_penalty(asset_id, asset_type)
        anomaly_penalty, recent_anomalies = self._calculate_anomaly_penalty(asset_id)

        total_penalty = alert_penalty + anomaly_penalty
        overall_score = max(0, weighted_score - total_penalty)

        # Get last updated time from metrics
        latest_metric = self.metrics.get_latest(f"{prefix}.quality", tags={"asset_id": asset_id})
        last_updated = latest_metric.timestamp if latest_metric else None

        score = HealthScore(
            asset_id=asset_id,
            asset_type=asset_type,
            overall_score=round(overall_score, 2),
            quality_score=round(quality_score, 2),
            freshness_score=round(freshness_score, 2),
            completeness_score=round(completeness_score, 2),
            reliability_score=round(reliability_score, 2),
            alert_penalty=round(alert_penalty, 2),
            anomaly_penalty=round(anomaly_penalty, 2),
            active_alerts=active_alerts,
            recent_anomalies=recent_anomalies,
            last_updated=last_updated,
            component_weights=weights
        )

        # Cache the score
        cache_key = f"{asset_type}:{asset_id}"
        self._scores_cache[cache_key] = score
        self._save_cache()

        return score

    def get_cached_health(self, asset_id: str, asset_type: str) -> Optional[HealthScore]:
        """
        Get cached health score for an asset.

        Args:
            asset_id: Asset identifier
            asset_type: Asset type

        Returns:
            Cached HealthScore or None
        """
        cache_key = f"{asset_type}:{asset_id}"
        return self._scores_cache.get(cache_key)

    def get_system_health(self) -> Dict[str, Any]:
        """
        Get overall system health dashboard.

        Returns:
            Dict with aggregated health metrics
        """
        all_scores = list(self._scores_cache.values())

        if not all_scores:
            return {
                "overall_health": 100.0,
                "asset_count": 0,
                "healthy_count": 0,
                "degraded_count": 0,
                "critical_count": 0,
                "active_alerts": self.alerts.get_alert_stats(),
                "by_asset_type": {},
                "lowest_scoring": [],
                "calculated_at": datetime.utcnow().isoformat()
            }

        # Calculate averages
        overall_avg = sum(s.overall_score for s in all_scores) / len(all_scores)

        # Count by health status
        healthy = [s for s in all_scores if s.overall_score >= 80]
        degraded = [s for s in all_scores if 50 <= s.overall_score < 80]
        critical = [s for s in all_scores if s.overall_score < 50]

        # Group by asset type
        by_type = {}
        for score in all_scores:
            if score.asset_type not in by_type:
                by_type[score.asset_type] = {"count": 0, "avg_score": 0, "scores": []}
            by_type[score.asset_type]["count"] += 1
            by_type[score.asset_type]["scores"].append(score.overall_score)

        for asset_type, data in by_type.items():
            data["avg_score"] = round(sum(data["scores"]) / len(data["scores"]), 2)
            del data["scores"]

        # Find lowest scoring assets
        sorted_scores = sorted(all_scores, key=lambda s: s.overall_score)
        lowest = [
            {
                "asset_id": s.asset_id,
                "asset_type": s.asset_type,
                "overall_score": s.overall_score,
                "active_alerts": s.active_alerts
            }
            for s in sorted_scores[:5]
        ]

        return {
            "overall_health": round(overall_avg, 2),
            "health_status": "healthy" if overall_avg >= 80 else ("degraded" if overall_avg >= 50 else "critical"),
            "asset_count": len(all_scores),
            "healthy_count": len(healthy),
            "degraded_count": len(degraded),
            "critical_count": len(critical),
            "active_alerts": self.alerts.get_alert_stats(),
            "by_asset_type": by_type,
            "lowest_scoring": lowest,
            "calculated_at": datetime.utcnow().isoformat()
        }

    def get_health_trend(
        self,
        asset_id: str,
        asset_type: str,
        hours: int = 168  # 1 week
    ) -> HealthTrend:
        """
        Get health score trend over time.

        Args:
            asset_id: Asset identifier
            asset_type: Asset type
            hours: Time window in hours

        Returns:
            HealthTrend with data points and trend direction
        """
        prefix = f"{asset_type}"

        # Get historical quality metrics as proxy for overall health
        metrics = self.metrics.query(f"{prefix}.quality", hours=hours)

        if not metrics:
            return HealthTrend(
                asset_id=asset_id,
                asset_type=asset_type,
                trend_direction="stable",
                change_percent=0.0
            )

        # Build data points
        data_points = [
            {"timestamp": m.timestamp.isoformat(), "score": m.value}
            for m in sorted(metrics, key=lambda m: m.timestamp)
        ]

        # Calculate trend
        if len(data_points) >= 2:
            first_score = data_points[0]["score"]
            last_score = data_points[-1]["score"]
            change_pct = ((last_score - first_score) / first_score * 100) if first_score > 0 else 0

            if change_pct > 5:
                direction = "improving"
            elif change_pct < -5:
                direction = "declining"
            else:
                direction = "stable"
        else:
            direction = "stable"
            change_pct = 0.0

        return HealthTrend(
            asset_id=asset_id,
            asset_type=asset_type,
            data_points=data_points,
            trend_direction=direction,
            change_percent=round(change_pct, 2)
        )

    def record_health_metrics(
        self,
        asset_id: str,
        asset_type: str,
        quality: float = None,
        freshness: float = None,
        completeness: float = None,
        reliability: float = None
    ) -> Dict[str, Any]:
        """
        Record health component metrics for an asset.

        Convenience method to record multiple health metrics at once.

        Args:
            asset_id: Asset identifier
            asset_type: Asset type
            quality: Quality score (0-100)
            freshness: Freshness score (0-100)
            completeness: Completeness score (0-100)
            reliability: Reliability score (0-100)

        Returns:
            Dict with recorded metrics
        """
        prefix = f"{asset_type}"
        tags = {"asset_id": asset_id, "asset_type": asset_type}
        recorded = []

        if quality is not None:
            self.metrics.record_value(f"{prefix}.quality", quality, tags=tags, unit="percent")
            recorded.append({"metric": f"{prefix}.quality", "value": quality})

        if freshness is not None:
            self.metrics.record_value(f"{prefix}.freshness", freshness, tags=tags, unit="percent")
            recorded.append({"metric": f"{prefix}.freshness", "value": freshness})

        if completeness is not None:
            self.metrics.record_value(f"{prefix}.completeness", completeness, tags=tags, unit="percent")
            recorded.append({"metric": f"{prefix}.completeness", "value": completeness})

        if reliability is not None:
            self.metrics.record_value(f"{prefix}.reliability", reliability, tags=tags, unit="percent")
            recorded.append({"metric": f"{prefix}.reliability", "value": reliability})

        return {
            "asset_id": asset_id,
            "asset_type": asset_type,
            "recorded_metrics": recorded,
            "timestamp": datetime.utcnow().isoformat()
        }

    def list_all_scores(self, asset_type: str = None) -> List[HealthScore]:
        """
        List all cached health scores.

        Args:
            asset_type: Optional filter by asset type

        Returns:
            List of HealthScore objects
        """
        scores = list(self._scores_cache.values())

        if asset_type:
            scores = [s for s in scores if s.asset_type == asset_type]

        return sorted(scores, key=lambda s: s.overall_score)

    def clear_cache(self) -> int:
        """
        Clear the health scores cache.

        Returns:
            Number of scores cleared
        """
        count = len(self._scores_cache)
        self._scores_cache.clear()
        self._save_cache()
        return count
