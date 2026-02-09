"""Data Observability - Metrics, alerting, and health monitoring for DataBridge AI Pro."""
from typing import Any


def register_observability_tools(mcp_instance: Any) -> None:
    """Register Data Observability tools with the MCP server."""
    try:
        from src.observability.mcp_tools import register_observability_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def obs_record_metric(asset: str, metric: str, value: float) -> str:
            """[Pro] Record a metric for a data asset.

            Args:
                asset: Asset identifier
                metric: Metric name
                value: Metric value

            Returns:
                Recording confirmation
            """
            return '{"error": "Observability tools require full Pro installation"}'

        @mcp_instance.tool()
        def obs_create_alert_rule(name: str, condition: str, severity: str = "warning") -> str:
            """[Pro] Create an alert rule.

            Args:
                name: Rule name
                condition: Alert condition
                severity: Alert severity (info, warning, critical)

            Returns:
                Alert rule configuration
            """
            return '{"error": "Observability tools require full Pro installation"}'

        @mcp_instance.tool()
        def obs_get_asset_health(asset: str) -> str:
            """[Pro] Get health status for a data asset.

            Args:
                asset: Asset identifier

            Returns:
                Asset health metrics
            """
            return '{"error": "Observability tools require full Pro installation"}'


__all__ = ['register_observability_tools']
