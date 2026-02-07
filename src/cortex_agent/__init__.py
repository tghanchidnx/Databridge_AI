"""
CortexAgent - Snowflake Cortex AI Integration for DataBridge.

This module provides:
1. Orchestrated Reasoning Loop - Observe → Plan → Execute → Reflect pattern
2. Direct Cortex Integration - Execute Cortex LLM functions via SQL
3. Communication Console - Full observability with CLI, File, Database outputs
4. State Management - Manage conversation state for stateless Cortex functions

Phase 19 of DataBridge AI.
"""

from .types import (
    MessageType,
    AgentState,
    CortexFunction,
    AgentMessage,
    ThinkingStep,
    ExecutionPlan,
    StepResult,
    CortexAgentConfig,
    Conversation,
    AgentResponse,
)
from .cortex_client import CortexClient
from .context import CortexAgentContext, get_context
from .console import (
    ConsoleOutput,
    CLIOutput,
    FileOutput,
    DatabaseOutput,
    CommunicationConsole,
)
from .reasoning_loop import CortexReasoningLoop

__all__ = [
    # Types
    "MessageType",
    "AgentState",
    "CortexFunction",
    "AgentMessage",
    "ThinkingStep",
    "ExecutionPlan",
    "StepResult",
    "CortexAgentConfig",
    "Conversation",
    "AgentResponse",
    # Core classes
    "CortexClient",
    "CortexAgentContext",
    "get_context",
    "CortexReasoningLoop",
    # Console
    "ConsoleOutput",
    "CLIOutput",
    "FileOutput",
    "DatabaseOutput",
    "CommunicationConsole",
]
