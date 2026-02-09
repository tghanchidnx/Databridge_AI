"""
Alert Manager - Threshold-based alerting and notification system.

Manages alert rules, evaluates metrics against thresholds,
tracks active alerts, and handles alert lifecycle.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from .types import (
    AlertRule, Alert, AlertStatus, AlertSeverity, Metric,
    compare_values, severity_to_weight
)


class AlertManager:
    """Manages alert rules and active alerts."""

    def __init__(self, data_dir: str = "data/observability"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.rules_file = self.data_dir / "alert_rules.json"
        self.alerts_file = self.data_dir / "alerts.json"
        self._rules: Dict[str, AlertRule] = {}
        self._alerts: Dict[str, Alert] = {}
        self._last_triggered: Dict[str, datetime] = {}  # rule_id -> last trigger time
        self._load()

    def _load(self) -> None:
        """Load rules and alerts from disk."""
        # Load rules
        if self.rules_file.exists():
            try:
                with open(self.rules_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for rule_data in data.get("rules", []):
                        rule = AlertRule(**rule_data)
                        self._rules[rule.id] = rule
            except (json.JSONDecodeError, ValueError):
                pass

        # Load alerts
        if self.alerts_file.exists():
            try:
                with open(self.alerts_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for alert_data in data.get("alerts", []):
                        alert = Alert(**alert_data)
                        self._alerts[alert.id] = alert
            except (json.JSONDecodeError, ValueError):
                pass

    def _save_rules(self) -> None:
        """Persist rules to disk."""
        data = {
            "rules": [rule.model_dump(mode="json") for rule in self._rules.values()]
        }
        with open(self.rules_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def _save_alerts(self) -> None:
        """Persist alerts to disk."""
        data = {
            "alerts": [alert.model_dump(mode="json") for alert in self._alerts.values()]
        }
        with open(self.alerts_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    # -------------------------------------------------------------------------
    # Alert Rule Management
    # -------------------------------------------------------------------------

    def create_rule(self, rule: AlertRule) -> AlertRule:
        """
        Create a new alert rule.

        Args:
            rule: AlertRule object to create

        Returns:
            The created AlertRule
        """
        self._rules[rule.id] = rule
        self._save_rules()
        return rule

    def create_rule_from_params(
        self,
        name: str,
        metric_name: str,
        threshold: float,
        comparison: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        duration_minutes: int = 5,
        description: str = "",
        enabled: bool = True,
        cooldown_minutes: int = 60
    ) -> AlertRule:
        """
        Create a rule from individual parameters.

        Args:
            name: Human-readable rule name
            metric_name: Name of the metric to monitor
            threshold: Threshold value to compare against
            comparison: Comparison operator (">", "<", ">=", "<=", "==")
            severity: Alert severity level
            duration_minutes: How long condition must persist
            description: Optional description
            enabled: Whether the rule is active

        Returns:
            The created AlertRule
        """
        rule = AlertRule(
            name=name,
            description=description,
            metric_name=metric_name,
            threshold=threshold,
            comparison=comparison,
            severity=severity,
            duration_minutes=duration_minutes,
            enabled=enabled,
            cooldown_minutes=cooldown_minutes
        )
        return self.create_rule(rule)

    def update_rule(self, rule_id: str, updates: Dict[str, Any]) -> Optional[AlertRule]:
        """
        Update an existing rule.

        Args:
            rule_id: ID of the rule to update
            updates: Dict of field names to new values

        Returns:
            Updated AlertRule or None if not found
        """
        if rule_id not in self._rules:
            return None

        rule = self._rules[rule_id]
        for key, value in updates.items():
            if hasattr(rule, key):
                setattr(rule, key, value)
        rule.updated_at = datetime.utcnow()
        self._save_rules()
        return rule

    def delete_rule(self, rule_id: str) -> bool:
        """
        Delete an alert rule.

        Args:
            rule_id: ID of the rule to delete

        Returns:
            True if deleted, False if not found
        """
        if rule_id not in self._rules:
            return False
        del self._rules[rule_id]
        self._save_rules()
        return True

    def get_rule(self, rule_id: str) -> Optional[AlertRule]:
        """Get a rule by ID."""
        return self._rules.get(rule_id)

    def list_rules(self, enabled_only: bool = False) -> List[AlertRule]:
        """
        List all alert rules.

        Args:
            enabled_only: If True, only return enabled rules

        Returns:
            List of AlertRule objects
        """
        rules = list(self._rules.values())
        if enabled_only:
            rules = [r for r in rules if r.enabled]
        return sorted(rules, key=lambda r: r.name)

    # -------------------------------------------------------------------------
    # Alert Evaluation
    # -------------------------------------------------------------------------

    def evaluate_metric(self, metric: Metric) -> Optional[Alert]:
        """
        Evaluate a metric against all matching rules.

        Args:
            metric: The metric to evaluate

        Returns:
            Alert if triggered, None otherwise
        """
        for rule in self._rules.values():
            if not rule.enabled:
                continue

            if rule.metric_name != metric.name:
                continue

            # Check tag filters
            if rule.tags:
                if not all(metric.tags.get(k) == v for k, v in rule.tags.items()):
                    continue

            # Check cooldown
            last_trigger = self._last_triggered.get(rule.id)
            if last_trigger:
                cooldown_end = last_trigger + timedelta(minutes=rule.cooldown_minutes)
                if datetime.utcnow() < cooldown_end:
                    continue

            # Evaluate threshold
            if compare_values(metric.value, rule.threshold, rule.comparison):
                # Create alert
                alert = Alert(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    severity=rule.severity,
                    metric_name=metric.name,
                    metric_value=metric.value,
                    threshold=rule.threshold,
                    comparison=rule.comparison,
                    message=f"{rule.name}: {metric.name} is {metric.value} (threshold: {rule.comparison} {rule.threshold})",
                    tags=metric.tags
                )
                self._alerts[alert.id] = alert
                self._last_triggered[rule.id] = datetime.utcnow()
                self._save_alerts()
                return alert

        return None

    # -------------------------------------------------------------------------
    # Alert Management
    # -------------------------------------------------------------------------

    def acknowledge(self, alert_id: str, user: str = None) -> bool:
        """
        Acknowledge an active alert.

        Args:
            alert_id: ID of the alert
            user: Optional user who acknowledged

        Returns:
            True if acknowledged, False if not found or already resolved
        """
        alert = self._alerts.get(alert_id)
        if not alert or alert.status == AlertStatus.RESOLVED:
            return False

        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.utcnow()
        alert.acknowledged_by = user
        self._save_alerts()
        return True

    def resolve(self, alert_id: str, user: str = None) -> bool:
        """
        Resolve an alert.

        Args:
            alert_id: ID of the alert
            user: Optional user who resolved

        Returns:
            True if resolved, False if not found
        """
        alert = self._alerts.get(alert_id)
        if not alert:
            return False

        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.utcnow()
        alert.resolved_by = user
        self._save_alerts()
        return True

    def get_alert(self, alert_id: str) -> Optional[Alert]:
        """Get an alert by ID."""
        return self._alerts.get(alert_id)

    def list_active(self) -> List[Alert]:
        """
        List all active (unresolved) alerts.

        Returns:
            List of active Alert objects sorted by severity and time
        """
        active = [a for a in self._alerts.values() if a.status != AlertStatus.RESOLVED]

        # Sort by severity (critical first) then by time (newest first)
        severity_order = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.WARNING: 1,
            AlertSeverity.INFO: 2
        }
        return sorted(active, key=lambda a: (severity_order.get(a.severity, 99), -a.triggered_at.timestamp()))

    def get_history(self, hours: int = 24, status: AlertStatus = None) -> List[Alert]:
        """
        Get historical alerts.

        Args:
            hours: Time window in hours
            status: Optional filter by status

        Returns:
            List of Alert objects
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        alerts = [a for a in self._alerts.values() if a.triggered_at >= cutoff]

        if status:
            alerts = [a for a in alerts if a.status == status]

        return sorted(alerts, key=lambda a: a.triggered_at, reverse=True)

    def get_alerts_by_metric(self, metric_name: str, active_only: bool = True) -> List[Alert]:
        """
        Get alerts for a specific metric.

        Args:
            metric_name: Name of the metric
            active_only: If True, only return active alerts

        Returns:
            List of Alert objects
        """
        alerts = [a for a in self._alerts.values() if a.metric_name == metric_name]

        if active_only:
            alerts = [a for a in alerts if a.status != AlertStatus.RESOLVED]

        return sorted(alerts, key=lambda a: a.triggered_at, reverse=True)

    def get_alert_stats(self) -> Dict[str, Any]:
        """
        Get alert statistics.

        Returns:
            Dict with counts by status, severity, etc.
        """
        all_alerts = list(self._alerts.values())
        active = [a for a in all_alerts if a.status != AlertStatus.RESOLVED]

        return {
            "total_alerts": len(all_alerts),
            "active_count": len([a for a in active if a.status == AlertStatus.ACTIVE]),
            "acknowledged_count": len([a for a in active if a.status == AlertStatus.ACKNOWLEDGED]),
            "resolved_count": len([a for a in all_alerts if a.status == AlertStatus.RESOLVED]),
            "by_severity": {
                "critical": len([a for a in active if a.severity == AlertSeverity.CRITICAL]),
                "warning": len([a for a in active if a.severity == AlertSeverity.WARNING]),
                "info": len([a for a in active if a.severity == AlertSeverity.INFO])
            },
            "total_rules": len(self._rules),
            "enabled_rules": len([r for r in self._rules.values() if r.enabled])
        }

    def calculate_alert_penalty(self) -> float:
        """
        Calculate total penalty from active alerts.

        Returns:
            Total penalty score (sum of severity weights)
        """
        active = self.list_active()
        return sum(severity_to_weight(a.severity) for a in active)

    def cleanup_old_alerts(self, days: int = 30) -> int:
        """
        Remove resolved alerts older than specified days.

        Args:
            days: Keep alerts from the last N days

        Returns:
            Number of alerts removed
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        to_remove = []

        for alert_id, alert in self._alerts.items():
            if alert.status == AlertStatus.RESOLVED:
                resolved_at = alert.resolved_at or alert.triggered_at
                if resolved_at < cutoff:
                    to_remove.append(alert_id)

        for alert_id in to_remove:
            del self._alerts[alert_id]

        if to_remove:
            self._save_alerts()

        return len(to_remove)
