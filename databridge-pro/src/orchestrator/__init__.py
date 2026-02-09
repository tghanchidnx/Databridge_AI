"""AI Orchestrator - Multi-agent task coordination for DataBridge AI Pro."""
from typing import Any


def register_orchestrator_tools(mcp_instance: Any) -> None:
    """Register AI Orchestrator tools with the MCP server."""
    try:
        from src.orchestrator.mcp_tools import register_orchestrator_tools as _register
        _register(mcp_instance)
    except ImportError:
        @mcp_instance.tool()
        def submit_orchestrated_task(task: str, agents: str = "") -> str:
            """[Pro] Submit a task for multi-agent orchestration.

            Args:
                task: Task description
                agents: Comma-separated agent names

            Returns:
                Task submission result
            """
            return '{"error": "Orchestrator tools require full Pro installation"}'

        @mcp_instance.tool()
        def get_orchestrator_health() -> str:
            """[Pro] Get orchestrator health status.

            Returns:
                Orchestrator health metrics
            """
            return '{"error": "Orchestrator tools require full Pro installation"}'


__all__ = ['register_orchestrator_tools']
