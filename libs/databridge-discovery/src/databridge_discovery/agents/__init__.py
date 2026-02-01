"""
Agents module for DataBridge Discovery.

This module provides the multi-agent architecture for automated workflows:
- Base agent interface
- Schema Scanner agent
- Logic Extractor agent
- Warehouse Architect agent
- Deploy & Validate agent
- Orchestrator for coordination
"""

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentConfig,
    AgentState,
    AgentCapability,
    AgentResult,
    AgentError,
)
from databridge_discovery.agents.schema_scanner import SchemaScanner
from databridge_discovery.agents.logic_extractor import LogicExtractor
from databridge_discovery.agents.warehouse_architect import WarehouseArchitect
from databridge_discovery.agents.deploy_validator import DeployValidator
from databridge_discovery.agents.orchestrator import (
    Orchestrator,
    OrchestratorConfig,
    WorkflowState,
)

__all__ = [
    # Base Agent
    "BaseAgent",
    "AgentConfig",
    "AgentState",
    "AgentCapability",
    "AgentResult",
    "AgentError",
    # Agents
    "SchemaScanner",
    "LogicExtractor",
    "WarehouseArchitect",
    "DeployValidator",
    # Orchestrator
    "Orchestrator",
    "OrchestratorConfig",
    "WorkflowState",
]
