"""Orchestrator module for AI Orchestrator integration.

This module provides MCP tools for interacting with the NestJS AI Orchestrator,
enabling task management, agent registration, and agent-to-agent messaging.
"""

from .mcp_tools import register_orchestrator_tools, OrchestratorClient

__all__ = ['register_orchestrator_tools', 'OrchestratorClient']
